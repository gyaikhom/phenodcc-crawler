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

import java.util.Comparator;
import org.mousephenotype.dcc.crawler.entities.FileSourceHasZip;

/**
 * Comparator used for comparing multiple file sources. This allows the crawler
 * to select the best file source for a given zip file. Each file source is
 * assigned a rating based on previous download behaviour.
 *
 * @author Gagarine Yaikhom <g.yaikhom@har.mrc.ac.uk>
 */
public class FileSourcePreferenceComparator implements Comparator<FileSourceHasZip> {

    @Override
    public int compare(FileSourceHasZip a, FileSourceHasZip b) {
        int returnValue = 0;
        short ra = a.getRating();
        short rb = b.getRating();
        if (ra > rb) {
            returnValue = 1;
        } else if (ra < rb) {
            returnValue = -1;
        }
        return returnValue;
    }
}
