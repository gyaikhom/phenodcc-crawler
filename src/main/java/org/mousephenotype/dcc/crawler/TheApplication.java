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

import java.io.File;
import java.util.Timer;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the main application which parses the command line options and starts
 * a crawling session.
 *
 * @author Gagarine Yaikhom <g.yaikhom@har.mrc.ac.uk>
 */
public final class TheApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(TheApplication.class);

    // used when converting periodicity to sleep time
    public static final long HOUR_TO_MILLISECS = 3600000L;

    // following are various parameters that affect the behaviour of the crawler.
    // Also included is the maximum and minimum values, and the default value
    // that should be used if the user did not specify a value on the command line
    public static final int DEFAULT_POOLSIZE = 10;
    public static final int MIN_POOLSIZE = 1;
    public static final int MAX_POOLSIZE = 10;
    public static final int DEFAULT_NUM_PARALLEL_DOWNLOADS = 1;
    public static final int MIN_NUM_PARALLEL_DOWNLOADS = 1;
    public static final int MAX_NUM_PARALLEL_DOWNLOADS = 10;
    public static final int DEFAULT_MAX_RETRIES = 1;
    public static final int MIN_MAX_RETRIES = 1;
    public static final int MAX_MAX_RETRIES = 5;
    public static final long DEFAULT_PERIODIC_DELAY_IN_HOURS = 0L;
    public static final long MIN_PERIODIC_DELAY_IN_HOURS = 0L;
    public static final String DEFAULT_DATA_DIR = "backup";

    // command line options and their switch keys
    private static final String OPT_HELP = "h";
    private static final String OPT_NUM_PARALLEL_DOWNLOADS = "a";
    private static final String OPT_MAX_RETRIES = "m";
    private static final String OPT_POOL_SIZE = "t";
    private static final String OPT_PERIODIC_DELAY = "p";
    private static final String OPT_DATA_DIR = "d";
    private static final String OPT_SEND_REPORT = "r";
    private static final String OPT_CRAWLER_PROP_FILE = "c";
    private static final String OPT_XMLSERIALISER_PROP_FILE = "s";
    private static final String OPT_XMLVALIDATOR_PROP_FILE = "v";
    private static final String OPT_XMLVALIDATION_RESOURCES_PROP_FILE = "x";
    private static final String OPT_CONTEXT_PROP_FILE = "o";
    private static final Options OPTIONS = new Options();

    // messages to show for each of the command line switches
    static {
        OPTIONS.addOption(OPT_HELP, false, "Show help message on how to use the system.");
        OPTIONS.addOption(OPT_NUM_PARALLEL_DOWNLOADS, true, "Number of parallel downloaders to use.");
        OPTIONS.addOption(OPT_MAX_RETRIES, true, "Maximum number of download retries.");
        OPTIONS.addOption(OPT_POOL_SIZE, true, "Maximum size of the thread pool.");
        OPTIONS.addOption(OPT_PERIODIC_DELAY, true, "Sets the delay (in hours) for periodic runs. If zero, the program returns immediately after processing has finished.");
        OPTIONS.addOption(OPT_DATA_DIR, true, "The path where the downloaded zipped data files will be stored. If unspecified, the current directory where the program is being executed is used.");
        OPTIONS.addOption(OPT_SEND_REPORT, true, "If you wish the crawler to send a report, use this switch and provide a valid email Id");
        OPTIONS.addOption(OPT_XMLSERIALISER_PROP_FILE, true, "The path to the properties file that specifies the XML serialiser configuration.");
        OPTIONS.addOption(OPT_XMLVALIDATOR_PROP_FILE, true, "The path to the properties file that specifies the XML validator configuration.");
        OPTIONS.addOption(OPT_XMLVALIDATION_RESOURCES_PROP_FILE, true, "The path to the properties file that specifies the XML validation resources configuration.");
        OPTIONS.addOption(OPT_CONTEXT_PROP_FILE, true, "The path to the properties file that specifies the context builder configuration.");
        OPTIONS.addOption(OPT_CRAWLER_PROP_FILE, true, "The path to the properties file that specifies the Crawler configuration.");
    }

    // used when showing usage information, or help message
    private static final int NUM_CHARS_PER_ROW = 120;

    // error/success codes when parsing the command line
    private static final int SUCCESS = 0;
    private static final int INVALID_NUM_PARALLEL_DOWNLOADS = 1;
    private static final int INVALID_MAX_RETRIES = 2;
    private static final int INVALID_POOL_SIZE = 3;
    private static final int INVALID_PERIODIC_DELAY = 4;
    private static final int INVALID_DATA_DIR = 5;
    private static final int INVALID_EMAIL_ID = 6;
    private static final int INVALID_CRAWLER_PROP_FILE = 7;
    private static final int INVALID_XMLSERIALISER_PROP_FILE = 8;
    private static final int INVALID_XMLVALIDATOR_PROP_FILE = 9;
    private static final int INVALID_XMLVALIDATION_RESOURCES_PROP_FILE = 10;
    private static final int INVALID_CONTEXT_PROP_FILE = 11;

    // initialise application configuration using the default values
    private int poolSize = DEFAULT_POOLSIZE;
    private int numParallelDownloaders = DEFAULT_NUM_PARALLEL_DOWNLOADS;
    private int maxRetries = DEFAULT_MAX_RETRIES;
    private long periodicDelayInHours = DEFAULT_PERIODIC_DELAY_IN_HOURS;
    private static String dataDirectory = DEFAULT_DATA_DIR;
    private boolean requiresReport = false;
    private String emailId = null;

    // these allow specification of custom target database connection
    // properties, which, if valid, will override the default properties
    // defined in:
    //
    // * mysql.properties of the exportLibrary.xmlserialization project, and
    // * persistence.xml of the phenodcc-crawler-entities project
    //
    // this project inherits these properties from the above projects. the
    // first is used for storing data to phenodcc_raw database, and the second
    // is used for storing tracking data in phenodcc_tracker database.
    private String crawlerPropFile = null;
    private String xmlSerialiserPropFile = null;
    private String xmlValidatorPropFile = null;
    private String xmlValidationResourcesPropFile = null;
    private String contextPropFile = null;

    // application variables
    private final StringBuilder header = new StringBuilder();
    private SessionInitiator processInitiator;

    private void showHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(NUM_CHARS_PER_ROW, "\n\njava -jar program.jar", null, OPTIONS, null, true);
    }

    private TheApplication() {
        header.append("\nPhenoDCC: File Source Crawler");
        header.append("\nCopyright (c) 2013 Medical Research Council Harwell");
        header.append("\n(http://www.mousephenotype.org)\n\n");
    }

    public static void main(String[] args) {
        SingleInstance si = SingleInstance.getInstance();
        switch (si.check()) {
            case 0:
                LOGGER.debug("No crawler instance is currently running... will start a new instance");
                TheApplication a = new TheApplication();
                a.setup(args);
                a.run();
                break;
            case 1:
                LOGGER.warn("Failure to acquire lock '{}'...", si.getLockPath());
                LOGGER.warn("An instance of the crawler seems to be running...");
                LOGGER.warn("If you kill this instance, you must delete the lock file '{}'", si.getLockPath());
                break;
            case 2:
                LOGGER.warn("Corrupt lock file path '{}'...", si.getLockPath());
                LOGGER.warn("Please verify and delete the lock file path '{}'...", si.getLockPath());
                break;
            default:
                break;
        }
        si.shutdown();
    }

    private void setup(String[] args) {
        LOGGER.info(header.toString());
        if (parseArgs(args)) {
            SettingsManager sm = SettingsManager.getInstance();
            sm.printSettings();
            if (createRequiredDirectories()) {
                processInitiator = new SessionInitiator(dataDirectory, numParallelDownloaders, poolSize, maxRetries);
            } else {
                exit("Failed to create required directories", false);
            }
        } else {
            exit("Error parsing commandline arguments", true);
        }
    }

    private void run() {
        if (periodicDelayInHours > 0) {
            Timer timer = new Timer();
            timer.schedule(processInitiator, 0L, periodicDelayInHours * HOUR_TO_MILLISECS);
        } else {
            processInitiator.run();
            if (requiresReport) {
                sendReport();
            }
            LOGGER.info("Application will now exit...");
        }
    }

    private void cleanupAndExit(int code) {
        SingleInstance si = SingleInstance.getInstance();
        si.shutdown();
        System.exit(code);
    }

    private void exit(String message, boolean h) {
        LOGGER.error(message + "\nApplication will now exit...\n");
        if (h) {
            showHelp();
        }
        cleanupAndExit(1);
    }

    private boolean parseArgs(String[] args) {
        boolean parseResult = false;
        try {
            CommandLineParser parser = new PosixParser();
            CommandLine cmd = parser.parse(OPTIONS, args);
            if (cmd.hasOption(OPT_HELP)) {
                showHelp();
                cleanupAndExit(0);
            } else {
                parseResult = true;
                parseResult &= (getDataDirectory(cmd) == SUCCESS);
                parseResult &= (getNumParallelDownloads(cmd) == SUCCESS);
                parseResult &= (getPoolSize(cmd) == SUCCESS);
                parseResult &= (getMaxRetries(cmd) == SUCCESS);
                parseResult &= (getPeriodicDelay(cmd) == SUCCESS);
                parseResult &= (getSendReport(cmd) == SUCCESS);
                parseResult &= (getCrawlerPropFile(cmd) == SUCCESS);
                parseResult &= (getXmlSerialiserPropFile(cmd) == SUCCESS);
                parseResult &= (getXmlValidatorPropFile(cmd) == SUCCESS);
                parseResult &= (getXmlValidationResourcesPropFile(cmd) == SUCCESS);
                parseResult &= (getContextPropFile(cmd) == SUCCESS);
            }
        } catch (ParseException e) {
            LOGGER.error(e.getMessage());
        }
        return parseResult;
    }

    private int getNumParallelDownloads(CommandLine cmd) {
        int parseResult = SUCCESS;
        if (cmd.hasOption(OPT_NUM_PARALLEL_DOWNLOADS)) {
            numParallelDownloaders = Integer.parseInt(cmd.getOptionValue(OPT_NUM_PARALLEL_DOWNLOADS));
            if (numParallelDownloaders < MIN_NUM_PARALLEL_DOWNLOADS
                    || numParallelDownloaders > MAX_NUM_PARALLEL_DOWNLOADS) {
                LOGGER.error("The number of parallel downloads should be greater than {} and less than {}.",
                        MIN_NUM_PARALLEL_DOWNLOADS, MAX_NUM_PARALLEL_DOWNLOADS);
                parseResult = INVALID_NUM_PARALLEL_DOWNLOADS;
            }
        }
        return parseResult;
    }

    private int getMaxRetries(CommandLine cmd) {
        int parseResult = SUCCESS;
        if (cmd.hasOption(OPT_MAX_RETRIES)) {
            maxRetries = Integer.parseInt(cmd.getOptionValue(OPT_MAX_RETRIES));
            if (maxRetries < MIN_MAX_RETRIES || maxRetries > MAX_MAX_RETRIES) {
                LOGGER.error("The maximum number of download retries should be greater than {} and less than {}.",
                        MIN_MAX_RETRIES, MAX_MAX_RETRIES);
                parseResult = INVALID_MAX_RETRIES;
            }
        }
        return parseResult;
    }

    private int getPoolSize(CommandLine cmd) {
        int parseResult = SUCCESS;
        if (cmd.hasOption(OPT_POOL_SIZE)) {
            poolSize = Integer.parseInt(cmd.getOptionValue(OPT_POOL_SIZE));
            if (poolSize < MIN_POOLSIZE || poolSize > MAX_POOLSIZE) {
                LOGGER.error("The maximum size of the thread pool should be greater than {} and less than {}.",
                        MIN_POOLSIZE, MAX_POOLSIZE);
                parseResult = INVALID_POOL_SIZE;
            }
        }
        return parseResult;
    }

    private int getPeriodicDelay(CommandLine cmd) {
        int parseResult = SUCCESS;
        if (cmd.hasOption(OPT_PERIODIC_DELAY)) {
            periodicDelayInHours = Integer.parseInt(cmd.getOptionValue(OPT_PERIODIC_DELAY));
            if (periodicDelayInHours < MIN_PERIODIC_DELAY_IN_HOURS) {
                LOGGER.error("The periodic delay should be either {} (non-periodic), or a positive number of hours.", MIN_PERIODIC_DELAY_IN_HOURS);
                parseResult = INVALID_PERIODIC_DELAY;
            }
        }
        return parseResult;
    }

    private int getDataDirectory(CommandLine cmd) {
        int parseResult = SUCCESS;
        if (cmd.hasOption(OPT_DATA_DIR)) {
            dataDirectory = cmd.getOptionValue(OPT_DATA_DIR);
            if (dataDirectory == null || dataDirectory.isEmpty()) {
                LOGGER.error("The supplied data storage directory is invalid.");
                parseResult = INVALID_DATA_DIR;
            }
        }
        return parseResult;
    }

    private File getReadableFile(String path) {
        File f = new File(path);
        if (f.exists()) {
            if (f.isFile()) {
                if (f.canRead()) {
                    LOGGER.info("Will use supplied properties file '{}'.", path);
                } else {
                    LOGGER.error("The supplied properties file '{}' is unreadable.", path);
                    f = null;
                }
            } else {
                LOGGER.error("The supplied properties path '{}' is not a file.", path);
                f = null;
            }
        } else {
            LOGGER.error("The supplied properties path '{}' does not exists.", path);
            f = null;
        }
        return f;
    }

    private int getCrawlerPropFile(CommandLine cmd) {
        int parseResult = SUCCESS;
        if (cmd.hasOption(OPT_CRAWLER_PROP_FILE)) {
            crawlerPropFile = cmd.getOptionValue(OPT_CRAWLER_PROP_FILE);
            if (crawlerPropFile == null || crawlerPropFile.isEmpty()) {
                LOGGER.error("The supplied properties file path for Crawler configuration is invalid.");
                parseResult = INVALID_CRAWLER_PROP_FILE;
            } else {
                File f = getReadableFile(crawlerPropFile);
                if (f == null) {
                    parseResult = INVALID_CRAWLER_PROP_FILE;
                } else {
                    SettingsManager sm = SettingsManager.getInstance();
                    sm.setCrawlerPropPath(f.getAbsolutePath());
                    sm.loadCustomConfig(f);
                }
            }
        } else {
            LOGGER.error("Please supply a properties file that specifies the Crawler configuration properties.");
            parseResult = INVALID_CRAWLER_PROP_FILE;
        }
        return parseResult;
    }

    private int getXmlSerialiserPropFile(CommandLine cmd) {
        int parseResult = SUCCESS;
        if (cmd.hasOption(OPT_XMLSERIALISER_PROP_FILE)) {
            xmlSerialiserPropFile = cmd.getOptionValue(OPT_XMLSERIALISER_PROP_FILE);
            if (xmlSerialiserPropFile == null || xmlSerialiserPropFile.isEmpty()) {
                LOGGER.error("The supplied properties file path for XML serialiser configuration is invalid.");
                parseResult = INVALID_XMLSERIALISER_PROP_FILE;
            } else {
                File f = getReadableFile(xmlSerialiserPropFile);
                if (f == null) {
                    parseResult = INVALID_XMLSERIALISER_PROP_FILE;
                } else {
                    SettingsManager sm = SettingsManager.getInstance();
                    sm.setXmlSerialiserPropPath(f.getAbsolutePath());
                }
            }
        }
        return parseResult;
    }

    private int getXmlValidatorPropFile(CommandLine cmd) {
        int parseResult = SUCCESS;
        if (cmd.hasOption(OPT_XMLVALIDATOR_PROP_FILE)) {
            xmlValidatorPropFile = cmd.getOptionValue(OPT_XMLVALIDATOR_PROP_FILE);
            if (xmlValidatorPropFile == null || xmlValidatorPropFile.isEmpty()) {
                LOGGER.error("The supplied properties file path for XML validator configuration is invalid.");
                parseResult = INVALID_XMLVALIDATOR_PROP_FILE;
            } else {
                File f = getReadableFile(xmlValidatorPropFile);
                if (f == null) {
                    parseResult = INVALID_XMLVALIDATOR_PROP_FILE;
                } else {
                    SettingsManager sm = SettingsManager.getInstance();
                    sm.setXmlValidatorPropPath(f.getAbsolutePath());
                }
            }
        }
        return parseResult;
    }

    private int getXmlValidationResourcesPropFile(CommandLine cmd) {
        int parseResult = SUCCESS;
        if (cmd.hasOption(OPT_XMLVALIDATION_RESOURCES_PROP_FILE)) {
            xmlValidationResourcesPropFile = cmd.getOptionValue(OPT_XMLVALIDATION_RESOURCES_PROP_FILE);
            if (xmlValidationResourcesPropFile == null || xmlValidationResourcesPropFile.isEmpty()) {
                LOGGER.error("The supplied properties file path for XML validation resources configuration is invalid.");
                parseResult = INVALID_XMLVALIDATION_RESOURCES_PROP_FILE;
            } else {
                File f = getReadableFile(xmlValidationResourcesPropFile);
                if (f == null) {
                    parseResult = INVALID_XMLVALIDATION_RESOURCES_PROP_FILE;
                } else {
                    SettingsManager sm = SettingsManager.getInstance();
                    sm.setXmlValidationResourcesPropPath(f.getAbsolutePath());
                }
            }
        }
        return parseResult;
    }

    private int getContextPropFile(CommandLine cmd) {
        int parseResult = SUCCESS;
        if (cmd.hasOption(OPT_CONTEXT_PROP_FILE)) {
            contextPropFile = cmd.getOptionValue(OPT_CONTEXT_PROP_FILE);
            if (contextPropFile == null || contextPropFile.isEmpty()) {
                LOGGER.error("The supplied properties file path for context builder configuration is invalid.");
                parseResult = INVALID_CONTEXT_PROP_FILE;
            } else {
                File f = getReadableFile(contextPropFile);
                if (f == null) {
                    parseResult = INVALID_CONTEXT_PROP_FILE;
                } else {
                    SettingsManager sm = SettingsManager.getInstance();
                    sm.setContextPropPath(f.getAbsolutePath());
                }
            }
        }
        return parseResult;
    }

    private int getSendReport(CommandLine cmd) {
        int parseResult = SUCCESS;
        if (cmd.hasOption(OPT_SEND_REPORT)) {
            requiresReport = true;
            emailId = cmd.getOptionValue(OPT_SEND_REPORT);
            if (emailId == null || emailId.isEmpty()) {
                LOGGER.error("The supplied email Id is invalid.");
                parseResult = INVALID_EMAIL_ID;
            }
        }
        return parseResult;
    }

    private boolean createRequiredDirectories() {
        boolean returnValue = false;
        if (createDirectory(dataDirectory + "/add", LOGGER)
                && createDirectory(dataDirectory + "/edit", LOGGER)
                && createDirectory(dataDirectory + "/delete", LOGGER)) {
            returnValue = true;
        }
        return returnValue;
    }

    public static boolean createDirectory(String contentsPath, Logger logger) {
        boolean returnValue = false;
        File f = new File(contentsPath);
        if (f.exists()) {
            if (f.isDirectory()) {
                if (f.canRead()) {
                    if (f.canWrite()) {
                        returnValue = true;
                    } else {
                        logger.error("Cannot write to directory '{}'", contentsPath);
                    }
                } else {
                    logger.error("Cannot read the directory '{}'", contentsPath);
                }
            } else {
                logger.error("Path '{}' is not a directory", contentsPath);
            }
        } else {
            logger.info("Path '{}' does not exists; will try to create", contentsPath);
            if (f.mkdirs()) {
                returnValue = true;
            } else {
                logger.error("Failed to create directory '{}'", contentsPath);
            }
        }
        return returnValue;
    }

    private void sendReport() {
        LOGGER.info("Send session report to '{}'...", emailId);
    }
}
