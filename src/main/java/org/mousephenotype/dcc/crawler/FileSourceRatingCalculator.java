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

import org.mousephenotype.dcc.crawler.entities.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used by the downloader to calculate the rating for a file source.
 * 
 * @author Gagarine Yaikhom <g.yaikhom@har.mrc.ac.uk>
 */
public final class FileSourceRatingCalculator {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSourceRatingCalculator.class);
    public static final short AFFINITY_WEIGHT = 30;

    private FileSourceRatingCalculator() {
    }

    public static int calculate(FileSourceHasZip f) {
        int returnValue = 0;
        if (f == null) {
            LOGGER.warn("Supplied FileSourceHasZip information is invalid");
        } else {
            FileSource s = f.getFileSourceId();
            if (s == null) {
                LOGGER.warn("Failed to retrieve file source information");
            } else {
                Centre c = s.getCentreId();
                if (c == null) {
                    LOGGER.warn("Failed to retrieve centre infomation");
                } else {
                    ZipAction za = f.getZaId();
                    if (za == null) {
                        LOGGER.warn("Failed to rerieve zip action");
                    } else {
                        ZipFile z = za.getZipId();
                        if (z == null) {
                            LOGGER.warn("Failed to retrieve zip file");
                        } else {
                            short r = 0;
                            if (c.equals(z.getCentreId())) {
                                r = AFFINITY_WEIGHT;
                            }
                            f.setRating(r);
                            returnValue = r;
                            LOGGER.debug("File source '{}' has rating '{}'", s.getHostname(), r);
                        }
                    }
                }
            }
        }
        return returnValue;
    }
}
