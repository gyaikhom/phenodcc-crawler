/*
 * Copyright 2013 Medical Research Council Harwell.
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

import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.mousephenotype.dcc.crawler.entities.Centre;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used by the XmlExtractor to parse file names. To parse a file name, the
 * FilenameTokenizer uses RegEx supplied by the user in tracker properties.
 * 
 * @author Gagarine Yaikhom <g.yaikhom@har.mrc.ac.uk>
 */
public class FilenameTokenizer {

    private final Logger logger = LoggerFactory.getLogger(FilenameTokenizer.class);
    private static FilenameTokenizer instance = null;
    private static Pattern zipPatternMatcher;
    private static Pattern xmlPatternMatcher;

    public synchronized static FilenameTokenizer getInstance() {
        if (instance == null) {
            instance = new FilenameTokenizer();
            SettingsManager sm = SettingsManager.getInstance();
            zipPatternMatcher = Pattern.compile(sm.getZipFileNameRegex());
            xmlPatternMatcher = Pattern.compile(sm.getXmlFileNameRegex());
        }
        return instance;
    }

    private Date getDate(Matcher m) {
        Date date = null;
        try {
            int year = Integer.parseInt(m.group(2));
            int month = Integer.parseInt(m.group(3));
            int day = Integer.parseInt(m.group(4));
            if ((month > 0 && month < 13) && (day > 0 && day < 32)) {
                Calendar cal = Calendar.getInstance();
                cal.set(year, month - 1, day);
                date = cal.getTime();
            }
        } catch (Exception e) {
        }
        return date;
    }

    private FilenameTokens getZipTokens(Matcher m) {
        DatabaseAccessor da = DatabaseAccessor.getInstance();
        FilenameTokens returnValue = null;
        try {
            Centre centre = da.getCentre(m.group(1));
            Date date = getDate(m);
            Long inc = Long.parseLong(m.group(5));

            if (centre != null && date != null && inc >= 0) {
                returnValue = new FilenameTokens(centre, date, inc, true);
            } else {
                logger.error("Invalid token values - centre: {}, date: {}, increment: {}", centre, date, inc);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return returnValue;
    }

    private FilenameTokens getXmlTokens(Matcher m) {
        DatabaseAccessor da = DatabaseAccessor.getInstance();
        FilenameTokens returnValue = null;
        try {
            Centre centre = da.getCentre(m.group(1));
            Date date = getDate(m);
            Long inc = Long.parseLong(m.group(5));
            if (centre != null && date != null && inc >= 0) {
                returnValue = new FilenameTokens(centre, date, inc, false,
                        "specimen".equals(m.group(6)));
            } else {
                logger.error("Invalid token values for '{}'", m.group(0));
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return returnValue;
    }
    
    public synchronized FilenameTokens tokenize(String filename) {
        FilenameTokens tokens = null;
        Matcher m = zipPatternMatcher.matcher(filename);
        if (m.find()) {
            tokens = getZipTokens(m);
        } else {
            m = xmlPatternMatcher.matcher(filename);
            if (m.find()) {
                tokens = getXmlTokens(m);
            }
        }
        return tokens;
    }
}
