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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import org.mousephenotype.dcc.crawler.entities.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The DataInserter class is a singleton class, an instance of which makes
 * changes to the QC database using data from a valid XML document.
 *
 * After all of the XML documents have been validated for XML schema and SOP,
 * they are passed to the singleton instance of DataInserter. For each of the
 * XML documents, it carries out data integrity checks before updating the QC
 * database. Since XML documents can manifest implicit data dependencies, the
 * temporal ordering of the changes as requested by the XML documents must be
 * observed. This means that, we cannot use multi-threading for data integrity
 * checks and QC database modifications.
 *
 * @author Gagarine Yaikhom <g.yaikhom@har.mrc.ac.uk>
 */
public class DataInserter {

    private final Logger logger = LoggerFactory.getLogger(DataInserter.class);

    // The following constants should match those used in data insertion tool
    private static final int SUCCESS = 0;
    private static final int FAILURE = 1;
    private static final int PARSE_ARGS_FAIL = 100;
    private static final int DB_PROPERTIES_FILE_NOT_FOUND = 101;
    private static final int MISSING_XML_FILE_PATH = 102;
    private static final int DB_ERROR_CONNECTION = 103;
    private static final int DB_ERROR_SERIALIZING = 104;
        
    private static DataInserter instance = null;
    private String dataDirectory;
    private DatabaseAccessor da;
    private final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
    private String xmlSerialiserPropPath = null;
    private String xmlValidatorPropPath = null;
    private String xmlValidationResourcesPropPath = null;
    private String contextPropPath = null;
    private String overviewBuilderDatabase = null;
    private String overviewBuilder = null;
    private List<XmlFile> xmlFiles = new ArrayList<>();
    private CrawlingSession session = null;
    private SessionTask currentTask = null;

    protected DataInserter() {
        SettingsManager sm = SettingsManager.getInstance();
        if (sm.hasCustomCrawlerSettings()) {
            overviewBuilderDatabase = sm.getOverviewDatabase();
            overviewBuilder = sm.getOverviewBuilder();
        }
        if (sm.hasCustomXmlSerialiserSettings()) {
            xmlSerialiserPropPath = sm.getXmlSerialiserPropPath();
        }
        if (sm.hasCustomXmlValidatorSettings()) {
            xmlValidatorPropPath = sm.getXmlValidatorPropPath();
        }
        if (sm.hasCustomXmlValidationResourcesSettings()) {
            xmlValidationResourcesPropPath = sm.getXmlValidationResourcesPropPath();
        }
        if (sm.hasCustomContextSettings()) {
            contextPropPath = sm.getContextPropPath();
        }
    }

    public static DataInserter getInstance() {
        if (instance == null) {
            instance = new DataInserter();
        }
        return instance;
    }

    private String getContentsPath(ZipDownload zd) {
        ZipAction za = zd.getZfId().getZaId();
        String fileName = za.getZipId().getFileName();
        String todo = za.getTodoId().getShortName();
        String fullPath = dataDirectory + "/" + todo + "/" + fileName;
        return fullPath + ".contents/";
    }

    private int executeTool(List<String> cmd) {
        int returnValue = FAILURE;
        ProcessBuilder pb = new ProcessBuilder(
                cmd.toArray(new String[cmd.size()]));
        pb.redirectErrorStream(true);
        try {
            Process process = pb.start();
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                logger.info(line);
            }
            try {
                process.waitFor();
                returnValue = process.exitValue();
            } catch (InterruptedException ex) {
                logger.error(ex.getMessage());
            }
        } catch (IOException ex) {
            logger.error(ex.getMessage());
        }
        return returnValue;
    }

    /**
     * Persists specimen or experiment data contained inside an XML document to
     * the PhenoDCC database.
     *
     * Note that this method executes the Data Inserter jar file in a separate
     * JVM. We had to take this approach due to library conflicts when using the
     * import library with the tracker entities library.
     *
     * @param xmlFileId This is the XML file identifier assigned to this XML
     * document by the tracker database. This allows the data inserter, and
     * therefore the import library, to correctly link issues relevant to this
     * XML document.
     * @param lastUpdate When did we begin processing the XML file.
     * @param xmlFilePath This is the full path of the XMl document.
     * @param isSampleSet This identifies the type of data that is contained
     * inside the XML document: true if the data is specimen information; false
     * otherwise.
     * @return
     */
    private int persistXml(Long xmlFileId, Date lastUpdate,
            String xmlFilePath, boolean isSampleSet) {
        int returnValue = SUCCESS;
        if (xmlSerialiserPropPath == null) {
            logger.warn("Skipping serialisation: Serialiser properties file "
                    + "'phenodcc_raw.properties' was not supplied");
        } else {
            currentTask = da.beginSessionTask(session, da.getPhase("upload"),
                    "XML file: " + xmlFilePath);
            List<String> argumentsList = new ArrayList<>();
            argumentsList.add("java");
            argumentsList.add("-Xms128m");
            argumentsList.add("-Xmx3g");
            argumentsList.add("-jar");
            argumentsList.add("phenodcc-serializer.jar");
            argumentsList.add("-t");
            argumentsList.add(xmlFileId.toString());
            argumentsList.add("-r");
            argumentsList.add(df.format(lastUpdate));
            argumentsList.add("-d");
            argumentsList.add(xmlSerialiserPropPath);
            argumentsList.add(isSampleSet ? "-s" : "-p");
            argumentsList.add(xmlFilePath);

            StringBuilder sb = new StringBuilder("java -Xms128m -Xmx3g -jar");
            sb.append(" phenodcc-serializer.jar ");
            sb.append(" -t ");
            sb.append(xmlFileId);
            sb.append(" -r ");
            sb.append(df.format(lastUpdate));
            sb.append(" -d ");
            sb.append(xmlSerialiserPropPath);
            sb.append(isSampleSet ? " -s " : " -p ");
            sb.append(xmlFilePath);
            logger.info(sb.toString());
            returnValue = executeTool(argumentsList);
            da.finishSessionTask(currentTask, (short) returnValue);
        }

        return returnValue;
    }

    /**
     * Validates XML document contents for data integrity.
     *
     * @param xmlFileId This is the XML file identifier assigned to this XML
     * document by the tracker database.
     * @return
     */
    private int checkDataIntegrity(Long xmlFileId) {
        int returnValue = SUCCESS;

        if (xmlSerialiserPropPath == null) {
            logger.warn("Skipping XML validation: Serialiser properties file "
                    + "'phenodcc_raw.properties' was not supplied");
        } else {
            if (xmlValidatorPropPath == null) {
                logger.warn("Skipping XML validation: Validator properties "
                        + "file 'phenodcc_xmlvalidationresources.properties' "
                        + "was not supplied");
            } else {
                currentTask = da.beginSessionTask(session, da.getPhase("data"),
                        "XML file id: " + xmlFileId);
                List<String> argumentsList = new ArrayList<>();
                argumentsList.add("java");
                argumentsList.add("-Xmx6g");
                argumentsList.add("-XX:+UseSerialGC");
                argumentsList.add("-jar");
                argumentsList.add("phenodcc-validator.jar");
                argumentsList.add("-t");
                argumentsList.add(xmlFileId.toString());
                argumentsList.add("-f");
                argumentsList.add(xmlValidatorPropPath);
                argumentsList.add("-h");
                argumentsList.add(xmlValidationResourcesPropPath);

                StringBuilder sb = new StringBuilder("java -Xmx6g -XX:+UseSerialGC -jar");
                sb.append(" phenodcc-validator.jar ");
                sb.append(" -t ");
                sb.append(xmlFileId);
                sb.append(" -f ");
                sb.append(xmlValidatorPropPath);
                sb.append(" -h ");
                sb.append(xmlValidationResourcesPropPath);
                logger.info(sb.toString());
                returnValue = executeTool(argumentsList);
                da.finishSessionTask(currentTask, (short) returnValue);
            }
        }

        return returnValue;
    }

    /**
     * Runs context builder.
     *
     * @param xmlFileId This is the XML file identifier assigned to this XML
     * document by the tracker database.
     * @return
     */
    private int buildContext(Long xmlFileId) {
        int returnValue = SUCCESS;
        if (contextPropPath == null) {
            logger.warn("Skipping context building: Context builder "
                    + "properties file 'phenodcc_context.properties' "
                    + "was not supplied");
        } else {
            currentTask = da.beginSessionTask(session, da.getPhase("context"),
                    "XML file id: " + xmlFileId);
            List<String> argumentsList = new ArrayList<>();
            argumentsList.add("java");
            argumentsList.add("-Xms128m");
            argumentsList.add("-Xmx6g");
            argumentsList.add("-jar");
            argumentsList.add("phenodcc-context.jar");
            argumentsList.add("-x");
            argumentsList.add(xmlFileId.toString());
            argumentsList.add("-r");
            argumentsList.add(contextPropPath);

            StringBuilder sb = new StringBuilder("java -Xms128m -Xmx6g -jar");
            sb.append(" phenodcc-context.jar ");
            sb.append(" -x ");
            sb.append(xmlFileId);
            sb.append(" -r ");
            sb.append(contextPropPath);
            logger.info(sb.toString());
            returnValue = executeTool(argumentsList);
            da.finishSessionTask(currentTask, (short) returnValue);
        }

        return returnValue;
    }

    private int filesForOverviewBuilding() {
        Phase p = da.getPhase(Phase.BUILD_OVERVIEWS);
        AStatus s = da.getStatus(AStatus.PENDING);
        xmlFiles = da.getXmlFilesByPhaseStatusTypeAscCreated(p, s, "%specimen%");
        xmlFiles.addAll(da.getXmlFilesByPhaseStatusTypeAscCreated(p, s, "%experiment%"));
        return xmlFiles.size();
    }

    /**
     * Build the overviews table.
     *
     * Note that this method executes the Overview Builder shell script in a
     * separate JVM.
     */
    private int buildOverviews() {
        int returnValue = SUCCESS, count;

        if (overviewBuilder == null) {
            logger.warn("Skipping overview building: Overview builder path "
                    + "not specified in 'phenodcc_tracker.properties'");
        } else {
            if (overviewBuilderDatabase == null) {
                logger.warn("Skipping overview building: Target overview "
                        + "database not specified in "
                        + "'phenodcc_tracker.properties'");
            } else {
                count = filesForOverviewBuilding();
                if (count > 0) {
                    currentTask = da.beginSessionTask(session, da.getPhase("overview"),
                            "Total number of XML files: " + count);
                    List<String> argumentsList = new ArrayList<>();

                    argumentsList.add(overviewBuilder);
                    argumentsList.add(overviewBuilderDatabase);

                    StringBuilder sb = new StringBuilder(overviewBuilder);
                    sb.append(" ");
                    sb.append(overviewBuilderDatabase);
                    logger.info(sb.toString());

                    updateOverviewStatus(AStatus.RUNNING);
                    returnValue = executeTool(argumentsList);
                    da.finishSessionTask(currentTask, (short) returnValue);
                }
            }
        }

        return returnValue;
    }

    private void logDataInsertionErrors(XmlFile xf, int v) {
        switch (v) {
            case PARSE_ARGS_FAIL:
                da.addXmlErrorLog(xf, "Unable to parse data inserter invocation arguments");
                break;
            case DB_PROPERTIES_FILE_NOT_FOUND:
                da.addXmlErrorLog(xf, "Unable to find or access database properties file");
                break;
            case DB_ERROR_CONNECTION:
                da.addXmlErrorLog(xf, "Unable to establish a connection with the database");
                break;
            case DB_ERROR_SERIALIZING:
                da.addXmlErrorLog(xf, "Unable to serialise the XML data to database");
                break;
            case MISSING_XML_FILE_PATH:
                da.addXmlErrorLog(xf, "Path of the XML document to process must be specified.");
                break;
            default:
        }
    }

    private boolean processXmlFile(XmlFile xf, boolean isSampleSet) {
        boolean returnValue = false;
        String path = getContentsPath(xf.getZipId()) + xf.getFname();
        if ((xf = da.setXmlFilePhaseStatus(xf, Phase.UPLOAD_DATA, AStatus.RUNNING)) != null) {
            int v = persistXml(xf.getId(), xf.getLastUpdate(), path, isSampleSet);
            if (SUCCESS == v) {
                da.setXmlFilePhaseStatus(xf, Phase.CHECK_DATA_INTEGRITY, AStatus.PENDING);
                returnValue = true;
            } else {
                da.setXmlFilePhaseStatus(xf, Phase.UPLOAD_DATA, AStatus.FAILED);
                logDataInsertionErrors(xf, v);
            }
        }
        return returnValue;
    }

    private boolean validateData(XmlFile xf) {
        boolean returnValue = false;
        if ((xf = da.setXmlFilePhaseStatus(xf, Phase.CHECK_DATA_INTEGRITY, AStatus.RUNNING)) != null) {
            int v = checkDataIntegrity(xf.getId());
            if (SUCCESS == v) {
                da.setXmlFilePhaseStatus(xf, Phase.BUILD_CONTEXT, AStatus.RUNNING);
                v = buildContext(xf.getId());
                if (SUCCESS == v) {
                    da.setXmlFilePhaseStatus(xf, Phase.BUILD_OVERVIEWS, AStatus.PENDING);
                } else {
                    da.setXmlFilePhaseStatus(xf, Phase.BUILD_CONTEXT, AStatus.FAILED);
                }
            } else {
                da.setXmlFilePhaseStatus(xf, Phase.CHECK_DATA_INTEGRITY, AStatus.FAILED);
            }
        }
        return returnValue;
    }

    private class FailureCounter {

        private int total = 0;
        private int failed = 0;

        public int getFailed() {
            return failed;
        }

        public void setFailed(int failed) {
            this.failed = failed;
        }

        public int getTotal() {
            return total;
        }

        public void setTotal(int total) {
            this.total = total;
        }
    }

    private FailureCounter processXmlDataSet(boolean isSampleSet) {
        Phase p = da.getPhase(Phase.VALIDATE_XML);
        AStatus s = da.getStatus(AStatus.DONE);
        List<XmlFile> xfl;

        if (isSampleSet) {
            xfl = da.getXmlFilesByPhaseStatusTypeAscCreated(p, s, "%specimen%");
        } else {
            xfl = da.getXmlFilesByPhaseStatusTypeAscCreated(p, s, "%experiment%");
        }

        // prepare the return object, which retains the failure information
        FailureCounter fc = new FailureCounter();
        fc.setTotal(xfl.size());
        int countFailed = 0;

        // process each of the validated XML documents
        Iterator<XmlFile> xi = xfl.iterator();
        while (xi.hasNext()) {
            XmlFile xf = xi.next();
            if (!processXmlFile(xf, isSampleSet)) {
                ++countFailed;
            }
        }

        // set the number of documents that failed
        fc.setFailed(countFailed);
        return fc;
    }

    private FailureCounter validateDataSet(boolean isSampleSet) {
        Phase p = da.getPhase(Phase.CHECK_DATA_INTEGRITY);
        AStatus s = da.getStatus(AStatus.PENDING);
        List<XmlFile> xfl;

        if (isSampleSet) {
            xfl = da.getXmlFilesByPhaseStatusTypeAscCreated(p, s, "%specimen%");
        } else {
            xfl = da.getXmlFilesByPhaseStatusTypeAscCreated(p, s, "%experiment%");
        }

        // prepare the return object, which retains the failure information
        FailureCounter fc = new FailureCounter();
        fc.setTotal(xfl.size());
        int countFailed = 0;

        // data integrity check and build context and overview tables
        Iterator<XmlFile> xi = xfl.iterator();
        while (xi.hasNext()) {
            XmlFile xf = xi.next();
            if (!validateData(xf)) {
                ++countFailed;
            }
        }

        // set the number of documents that failed
        fc.setFailed(countFailed);
        return fc;
    }

    private boolean insertSpecimens() {
        boolean anyFailures = false;
        FailureCounter fc = processXmlDataSet(true);
        if (fc.getTotal() == 0) {
            logger.info("Did not find any XSD validated specimen XML documents");
        } else {
            anyFailures = (fc.getFailed() > 0);
            if (anyFailures) {
                logger.warn("{} out of {} specimen XML documents failed upload!",
                        fc.getFailed(), fc.getTotal());
            }
        }
        return anyFailures;
    }

    private boolean insertProcedureResults() {
        boolean anyFailures = false;
        FailureCounter fc = processXmlDataSet(false);
        if (fc.getTotal() == 0) {
            logger.info("Did not find any XSD validated procedure XML documents");
        } else {
            anyFailures = (fc.getFailed() > 0);
            if (anyFailures) {
                logger.warn("{} out of {} procedure XML documents failed upload!",
                        fc.getFailed(), fc.getTotal());
            }
        }
        return anyFailures;
    }

    private boolean validateSpecimens() {
        boolean anyFailures = false;
        FailureCounter fc = validateDataSet(true);
        if (fc.getTotal() == 0) {
            logger.info("Did not find newly uploaded specimen data files");
        } else {
            anyFailures = (fc.getFailed() > 0);
            if (anyFailures) {
                logger.warn("{} out of {} specimen data files are invalid!",
                        fc.getFailed(), fc.getTotal());
            }
        }
        return anyFailures;
    }

    private boolean validateProcedureResults() {
        boolean anyFailures = false;
        FailureCounter fc = validateDataSet(false);
        if (fc.getTotal() == 0) {
            logger.info("Did not find newly uploaded procedure data files");
        } else {
            anyFailures = (fc.getFailed() > 0);
            if (anyFailures) {
                logger.warn("{} out of {} procedure data files are invalid!",
                        fc.getFailed(), fc.getTotal());
            }
        }
        return anyFailures;
    }

    private void updateOverviewStatus(String status) {
        Iterator<XmlFile> i = xmlFiles.iterator();
        while (i.hasNext()) {
            XmlFile xf = i.next();
            da.setXmlFilePhaseStatus(xf,
                    Phase.BUILD_OVERVIEWS, status);
        }
    }

    public short updateQCDatabase(String dataDirectory,
            CrawlingSession session) {
        this.dataDirectory = dataDirectory;
        this.session = session;
        da = DatabaseAccessor.getInstance();
        logger.info("Will attempt to update QC database");

        logger.info("Uploading data from XML documents to 'phenodcc_raw'");

        // Insert specimen data before inserting procedure results.
        boolean specimenFailure = insertSpecimens();
        boolean procedureFailure = insertProcedureResults();
        boolean anyFailures = (specimenFailure || procedureFailure);

        logger.info("Validating recently uploaded data in 'phenodcc_raw'");
        specimenFailure = validateSpecimens();
        procedureFailure = validateProcedureResults();
        anyFailures = (anyFailures || specimenFailure || procedureFailure);

        logger.info("Building overviews");
        int v = buildOverviews();
        if (SUCCESS == v) {
            updateOverviewStatus(AStatus.DONE);
        } else {
            updateOverviewStatus(AStatus.FAILED);
        }
        anyFailures = (anyFailures || SUCCESS != v);

        StringBuilder sb = new StringBuilder("Finished updating the QC database ");
        sb.append(anyFailures ? "with" : "without");
        sb.append(" errors...");
        if (anyFailures) {
            logger.warn(sb.toString());
        } else {
            logger.info(sb.toString());
        }
        return (short) (anyFailures ? FAILURE : SUCCESS);
    }
}
