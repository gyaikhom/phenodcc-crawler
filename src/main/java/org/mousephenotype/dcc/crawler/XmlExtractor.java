/*
 * Copyright 2012 Medical Research Council Harwell.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.mousephenotype.dcc.crawler;

import java.io.*;
import java.util.Enumeration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.mousephenotype.dcc.crawler.entities.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An instance of the XmlExtractor is responsible for extracting and processing
 * the contents of a zip file. The thread running this instance is created by
 * the thread running an instance of Downloader.
 * 
 * Each of the zip files are extracted in the file storage location specified
 * by the user at the command line. Inside this directory, the contents of a
 * zip file is extracted in its own directory, thus allowing multiple zip files
 * to have contents with the same file name. The name of this extraction
 * directory is the zip file name with '.contents' suffix. E.g., if we are
 * processing a zip file named abcd.zip, then the extraction directory will
 * be abcd.zip.contents.
 * 
 * From the zip file contents, only XML documents that conforms to the IMPC
 * XML file naming convention are processed. The rest are discarded. To process
 * each of these XML documents, the XmlExtractor spawns multiple threads for
 * each of the valid XML documents, each of which running an instance of
 * XmlValidator. This carries out the XSD validation of the document.
 * 
 * @author Gagarine Yaikhom <g.yaikhom@har.mrc.ac.uk>
 */
public class XmlExtractor implements Runnable {

    private static final int BUFFER_SIZE = 1024;
    private final Logger logger = LoggerFactory.getLogger(XmlExtractor.class);
    private static final String CONTENTS_SUFFIX = ".contents/";
    private String backupDir;
    private String fullPath;
    private String contentsPath;
    private ZipDownload zipDownload;
    private DatabaseAccessor da;

    XmlExtractor(String backupDir, ZipDownload zipDownload) {
        this.backupDir = backupDir;
        this.zipDownload = zipDownload;
    }

    @Override
    public void run() {
        da = DatabaseAccessor.getInstance();
        if (prepare()) {
            try {
                process();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        } else {
            logger.error("Failed to prepare extraction directory for '{}'", fullPath);
            da.setZipDownloadPhaseStatus(zipDownload, Phase.EXTRACT_XML, AStatus.FAILED);
        }
    }

    private boolean prepare() {
        ZipAction za = zipDownload.getZfId().getZaId();
        String fileName = za.getZipId().getFileName();
        String todo = za.getTodoId().getShortName();
        fullPath = backupDir + "/" + todo + "/" + fileName;
        contentsPath = fullPath + CONTENTS_SUFFIX;
        logger.debug("Starting extraction of '{}'", fullPath);
        da.setZipDownloadPhaseStatus(zipDownload, Phase.EXTRACT_XML, AStatus.RUNNING);
        return TheApplication.createDirectory(contentsPath, logger);
    }

    private XmlFile retrieveOrCreateXmlFile(ZipEntry ze,
            FilenameTokenizer t, String phase, String status) {
        String filename = ze.getName();
        XmlFile xf = da.getXmlFile(zipDownload, filename);
        if (xf == null) {
            xf = new XmlFile(zipDownload, filename,
                    da.getPhase(phase), da.getStatus(status));
            FilenameTokens tokens = t.tokenize(filename);
            if (tokens != null) {
                xf.setCentreId(tokens.getProducer());
                xf.setCreated(tokens.getCreated());
                xf.setInc(tokens.getInc());
                long size = ze.getSize();
                xf.setSizeBytes(size < 0 ? 0 : size);
            }
            da.persist(xf);
        }
        return xf;
    }

    private XmlFile processZipEntry(ZipEntry ze, FilenameTokenizer t) {
        XmlFile returnValue = null;
        String xmlName = ze.getName();
        if (ze.isDirectory()) {
            logger.warn("Ignoring directory '{}' in zip file '{}'", xmlName, fullPath);
        } else {
            if (xmlName.endsWith(".xml") && !xmlName.contains("/")) {
                XmlFile xf = retrieveOrCreateXmlFile(ze, t,
                        Phase.CHECK_XML_FILENAME, AStatus.RUNNING);
                if (xf.getCentreId() == null) {
                    logger.error("Xml file with name '{}' does not conform to naming convention", ze.getName());
                    da.setXmlFilePhaseStatus(xf, Phase.CHECK_XML_FILENAME, AStatus.FAILED);
                } else {
                    da.setXmlFilePhaseStatus(xf, Phase.CHECK_XML_FILENAME, AStatus.DONE);
                    returnValue = xf;
                }
            }
        }
        return returnValue;
    }

    private String extractXmlDocument(InputStream in, String fn) throws IOException {
        String xmlPath = contentsPath + fn;
        OutputStream out = null;
        try {
            FileOutputStream fos = new FileOutputStream(xmlPath);
            out = new BufferedOutputStream(fos);
            byte[] buffer = new byte[BUFFER_SIZE];
            int len;
            while ((len = in.read(buffer)) >= 0) {
                out.write(buffer, 0, len);
            }
            return xmlPath;
        } finally {
            if (in != null) {
                in.close();
            }
            if (out != null) {
                out.close();
            }
        }
    }

    private void process() throws InterruptedException {
        ExecutorService workers = Executors.newCachedThreadPool();
        try {
            ZipFile z = new ZipFile(fullPath);
            Enumeration<? extends ZipEntry> e = z.entries();
            FilenameTokenizer t = FilenameTokenizer.getInstance();
            while (e.hasMoreElements()) {
                ZipEntry ze = (ZipEntry) e.nextElement();
                XmlFile x = processZipEntry(ze, t);
                if (x != null) {
                    da.setXmlFilePhaseStatus(x, Phase.EXTRACT_XML, AStatus.RUNNING);
                    String xp = extractXmlDocument(z.getInputStream(ze), ze.getName());
                    if (xp == null) {
                        logger.error("Failed to extract file '{}'", ze.getName());
                        da.setXmlFilePhaseStatus(x, Phase.EXTRACT_XML, AStatus.FAILED);
                    } else {
                        da.setXmlFilePhaseStatus(x, Phase.EXTRACT_XML, AStatus.DONE);
                        workers.submit(new XmlValidator(x, xp));
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Failed to decompress zip file '{}'", fullPath);
            logger.error(e.getMessage());
            da.setZipDownloadPhaseStatus(zipDownload, Phase.EXTRACT_XML, AStatus.FAILED);
            da.addZipErrorLog(zipDownload, e);
        }
        workers.shutdown();
        workers.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }
}
