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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SettingsManager is a singleton class which is responsible for maintaining
 * the user supplied crawler settings. These settings are retrieved from the
 * command line options that were supplied by the user when the crawler was
 * executed.
 *
 * @author Gagarine Yaikhom <g.yaikhom@har.mrc.ac.uk>
 */
public class SettingsManager {

    private final Logger logger = LoggerFactory.getLogger(SettingsManager.class);
    private static final String DEFAULT_PROPERTIES = "org/mousephenotype/dcc/crawler/resources/tracker.properties";
    private static SettingsManager instance = null;
    private Properties prop;
    private String xmlSerialiserPropPath;
    private String xmlValidatorPropPath;
    private String contextPropPath;
    private boolean customCrawlerSettings;
    private boolean customXmlSerialiserSettings;
    private boolean customXmlValidatorSettings;
    private boolean customContextSettings;

    protected SettingsManager() {
        customCrawlerSettings = false;
        customXmlSerialiserSettings = false;
        customXmlValidatorSettings = false;
        customContextSettings = false;
    }

    public static SettingsManager getInstance() {
        if (instance == null) {
            instance = new SettingsManager();
            instance.loadDefaultConfig();
        }
        return instance;
    }

    private boolean load(InputStream propertiesInputStream) {
        boolean returnValue = false;
        this.prop = new Properties();
        try {
            if (propertiesInputStream == null) {
                logger.error("Failed to open Crawler configuration properties file");
            } else {
                prop.load(propertiesInputStream);
                returnValue = true;
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return returnValue;
    }

    private boolean loadDefaultConfig() {
        InputStream in = this.getClass()
                .getClassLoader()
                .getResourceAsStream(DEFAULT_PROPERTIES);
        return load(in);
    }

    public boolean loadCustomConfig(File f) {
        boolean returnValue = false;
        try {
            InputStream in = new FileInputStream(f);
            returnValue = load(in);
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage());
        }
        customCrawlerSettings = returnValue;
        return returnValue;
    }

    public String getZipFileNameRegex() {
        return prop.getProperty("phenodcc.file.name.regex.zip");
    }

    public String getXmlFileNameRegex() {
        return prop.getProperty("phenodcc.file.name.regex.xml");
    }

    public String getDriver() {
        return prop.getProperty("phenodcc.database.tracker.driver");
    }

    public String getOverviewDatabase() {
        return prop.getProperty("phenodcc.overview.database");
    }

    public String getOverviewBuilder() {
        return prop.getProperty("phenodcc.overview.builder");
    }

    public String getTrackerUrl() {
        return prop.getProperty("phenodcc.database.tracker.url");
    }

    public String getTrackerUser() {
        return prop.getProperty("phenodcc.database.tracker.user");
    }

    public String getTrackerPassword() {
        return prop.getProperty("phenodcc.database.tracker.password");
    }

    public String getXmlSerialiserPropPath() {
        return xmlSerialiserPropPath;
    }

    public void setXmlSerialiserPropPath(String propPath) {
        customXmlSerialiserSettings = true;
        xmlSerialiserPropPath = propPath;
    }

    public String getXmlValidatorPropPath() {
        return xmlValidatorPropPath;
    }

    public void setXmlValidatorPropPath(String propPath) {
        customXmlValidatorSettings = true;
        xmlValidatorPropPath = propPath;
    }

    public String getContextPropPath() {
        return contextPropPath;
    }

    public void setContextPropPath(String propPath) {
        customContextSettings = true;
        contextPropPath = propPath;
    }

    public boolean hasCustomCrawlerSettings() {
        return customCrawlerSettings;
    }

    public boolean hasCustomXmlSerialiserSettings() {
        return customXmlSerialiserSettings;
    }

    public boolean hasCustomXmlValidatorSettings() {
        return customXmlValidatorSettings;
    }

    public boolean hasCustomContextSettings() {
        return customContextSettings;
    }
}
