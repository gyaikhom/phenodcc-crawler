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

import java.util.Date;
import org.mousephenotype.dcc.crawler.entities.Centre;

/**
 * The tokens that are recognised as part of the IMPC file naming convention.
 *
 * @author Gagarine Yaikhom <g.yaikhom@har.mrc.ac.uk>
 */
public class FilenameTokens {

    private Centre producer; // centre that produced the data (not data host)
    private Date created; // date on which the data was released
    private Long inc; // increment number for the data for that release data
    private boolean zip; // is this zip file?
    private boolean specimen; // is this specimen data? only if not zip file

    public FilenameTokens(Centre producer, Date created, Long inc,
            boolean zip) {
        this.producer = producer;
        this.created = created;
        this.inc = inc;
        this.zip = zip;
        this.specimen = false;
    }

    public FilenameTokens(Centre producer, Date created, Long inc,
            boolean zip, boolean specimen) {
        this.producer = producer;
        this.created = created;
        this.inc = inc;
        this.zip = zip;
        this.specimen = specimen;
    }

    public Centre getProducer() {
        return producer;
    }

    public void setProducer(Centre producer) {
        this.producer = producer;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public Long getInc() {
        return inc;
    }

    public void setInc(Long inc) {
        this.inc = inc;
    }

    public boolean isSpecimen() {
        return specimen;
    }

    public void setSpecimen(boolean specimen) {
        this.specimen = specimen;
    }

    public boolean isZip() {
        return zip;
    }

    public void setZip(boolean zip) {
        this.zip = zip;
    }
}
