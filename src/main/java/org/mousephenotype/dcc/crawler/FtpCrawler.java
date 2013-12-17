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

import java.io.IOException;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.mousephenotype.dcc.crawler.entities.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An instance of FtpCrawler is responsible for crawling the data directories
 * on a specific FTP server. A thread that runs an instance of this class is
 * created by the DownloadManager for each of the file sources hosted by the
 * centres. Hence, multiple FTP servers are crawled simultaneously.
 * 
 * During crawling, the FtpCrawler visits the add, edit and delete directories
 * in the root path specific in the file source specification. Only the zip
 * files in these directories are processed, and of these zip files, only those
 * that conform to the IMPC file naming convention are added to the file
 * download map.
 *
 * @author Gagarine Yaikhom <g.yaikhom@har.mrc.ac.uk>
 */
public class FtpCrawler implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(FtpCrawler.class);
    private Centre centre;
    private String hostname;
    private FTPClient client;
    private FileSource fileSource;
    private ZipFileFilter zipFilter;
    private String basePath;

    FtpCrawler(Centre centre, FileSource fileSource) {
        this.centre = centre;
        this.fileSource = fileSource;
        basePath = fileSource.getBasePath();
    }

    @Override
    public void run() {
        try {
            if (fileSource != null) {
                hostname = fileSource.getHostname();
            } else {
                logger.error("Invalid hostname; will stop crawling");
                return;
            }
            logger.debug("Will start crawling '{}'", hostname);
            if (establishConnection()) {
                zipFilter = new ZipFileFilter();
                getFilesThatAddData();
                getFilesThatEditData();
                getFilesThatDeleteData();
            } else {
                logger.error("Ftp crawler could not established connection with server '{}'", hostname);
            }
            logger.debug("Crawling at '{}' has finished", hostname);
        } finally {
            if (client.isConnected()) {
                try {
                    client.logout();
                    client.disconnect();
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

    private boolean establishConnection() {
        boolean returnValue = false;
        String username, password;

        username = fileSource.getUsername();
        password = fileSource.getAccesskey();

        if (username == null || username.isEmpty()
                || password == null || password.isEmpty()) {
            logger.error("Invalid credential for server '{}' at centre '{}'",
                    hostname, centre.getShortName());
        } else {
            if (connect(username, password)) {
                logger.debug("Ftp crawler has established connection with server '{}'", hostname);
                returnValue = true;
            }
        }
        return returnValue;
    }

    private boolean connect(String username, String password) {
        boolean returnValue = false;
        try {
            logger.debug("Crawler is trying to connect to '{}'", hostname);
            client = new FTPClient();
            client.connect(hostname);
            client.login(username, password);
            int reply = client.getReplyCode();
            if (FTPReply.isPositiveCompletion(reply)) {
                logger.debug("Crawler is connected to ftp server at '{}'", hostname);
                returnValue = true;
            } else {
                logger.error("Server at '{}' refused connection", hostname);
                client.disconnect();
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return returnValue;
    }

    private ZipFile retrieveOrCreateZipFile(FilenameTokenizer t, FTPFile f) {
        DatabaseAccessor da = DatabaseAccessor.getInstance();
        String filename = f.getName();
        ZipFile z = da.getZipFile(filename);
        if (z == null) {
            FilenameTokens tokens = t.tokenize(filename);
            z = new ZipFile(filename);
            if (tokens != null) {
                z.setCentreId(tokens.getProducer());
                z.setCreated(tokens.getCreated());
                z.setInc(tokens.getInc());
                z.setSizeBytes(f.getSize());
            }
            da.persist(z);
        }
        return z;
    }

    private ZipAction retrieveOrCreateZipAction(ZipFile z, String todo, String phase, String status) {
        DatabaseAccessor da = DatabaseAccessor.getInstance();
        ZipAction za = da.getZipAction(z, todo);
        if (za == null) {
            Phase p = da.getPhase(phase);
            AStatus s = da.getStatus(status);
            ProcessingType pt = da.getProcessingType(todo);
            za = new ZipAction(z, pt, p, s);
            da.persist(za);
        }
        return za;
    }

    private int queueFiles(FTPFile[] files, String todo) {
        DatabaseAccessor da = DatabaseAccessor.getInstance();
        FilenameTokenizer tokenizer = FilenameTokenizer.getInstance();
        int c = 0;
        for (FTPFile f : files) {
            ZipFile z = retrieveOrCreateZipFile(tokenizer, f);
            ZipAction za = retrieveOrCreateZipAction(z, todo, Phase.CHECK_ZIP_FILENAME, AStatus.RUNNING);
            if (z.getCentreId() == null) {
                logger.error("Zip file with name '{}' does not conform to naming convention", z.getFileName());
                da.setZipActionPhaseStatus(za, Phase.CHECK_ZIP_FILENAME, AStatus.FAILED);
            } else {
                da.setZipActionPhaseStatus(za, Phase.CHECK_ZIP_FILENAME, AStatus.DONE);
            }

            FileSourceHasZip zf = da.getFileSourceHasZip(fileSource, za);
            if (zf == null) {
                zf = new FileSourceHasZip(fileSource, za);
                da.persist(zf);
                ++c;
            }
        }
        return c;
    }

    private int crawlPath(String action) {
        int c = 0;
        try {
            FTPFile[] files = client.listFiles(basePath + action, zipFilter);
            if (files.length > 0) {
                c = queueFiles(files, ZipAction.ADD_ACTION);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return c;
    }

    private int getFilesThatAddData() {
        logger.debug("Starting to crawl 'add' directory at {}", hostname);
        int c = crawlPath(ZipAction.ADD_ACTION);
        logger.debug("Finished crawling 'add' directory at {}", hostname);
        return c;
    }

    private int getFilesThatEditData() {
        logger.debug("Starting to crawl 'edit' directory at {}", hostname);
        int c = crawlPath(ZipAction.EDIT_ACTION);
        logger.debug("Finished crawling 'edit' directory at {}", hostname);
        return c;
    }

    private int getFilesThatDeleteData() {
        logger.debug("Starting to crawl 'delete' directory at {}", hostname);
        int c = crawlPath(ZipAction.DELETE_ACTION);
        logger.debug("Finished crawling 'delete' directory at {}", hostname);
        return c;
    }
}
