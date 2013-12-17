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
import java.util.List;
import org.mousephenotype.dcc.crawler.entities.AStatus;
import org.mousephenotype.dcc.crawler.entities.Phase;
import org.mousephenotype.dcc.crawler.entities.XmlFile;
import org.mousephenotype.dcc.exportlibrary.xsdvalidation.controls.ProcedureResultsXMLValidityChecker;
import org.mousephenotype.dcc.exportlibrary.xsdvalidation.controls.SpecimenResultsXMLValidityChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * An instance of XmlValidator carries out XSD validation of an XML file. The
 * IMPC uses standardised document schema specified using XSD. Any XML document
 * submitted to the DCC must conform to this schema. This is the leaf of the
 * multi-threaded process hierarchy, rooted at the DownloadManager.
 * 
 * @author Gagarine Yaikhom <g.yaikhom@har.mrc.ac.uk>
 */
public class XmlValidator implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(XmlValidator.class);
    private XmlFile xmlFile;
    private String fullPath;
    private DatabaseAccessor da;

    XmlValidator(XmlFile xmlFile, String fullPath) {
        this.xmlFile = xmlFile;
        this.fullPath = fullPath;
    }

    @Override
    public void run() {
        da = DatabaseAccessor.getInstance();
        logger.debug("Starting validation of '{}'", fullPath);
        da.setXmlFilePhaseStatus(xmlFile, Phase.VALIDATE_XML, AStatus.RUNNING);
        boolean result;

        if (xmlFile.getFname().contains("experiment")) {
            result = validateProcedureResults();
        } else {
            result = validateSampleResults();
        }

        if (result) {
            logger.debug("XML document '{}' is valid", fullPath);
            da.setXmlFilePhaseStatus(xmlFile, Phase.VALIDATE_XML, AStatus.DONE);
        } else {
            logger.debug("XML document '{}' is invalid", fullPath);
            da.setXmlFilePhaseStatus(xmlFile, Phase.VALIDATE_XML, AStatus.FAILED);
        }
    }

    private boolean validateSampleResults() {
        boolean returnValue = false;
        try {
            SpecimenResultsXMLValidityChecker validator = new SpecimenResultsXMLValidityChecker();
            validator.attachErrorHandler();
            validator.check(fullPath);
            if (validator.errorsFound()) {
                logger.debug("Sample XML document '{}' contains errors", fullPath);
                List<SAXParseException> issues = validator.getHandler().getExceptionMessages();
                if (issues != null && !issues.isEmpty()) {
                    da.addXmlErrorLogs(xmlFile, issues);
                }
            } else {
                returnValue = true;
            }
        } catch (IOException e) {
            logger.error("Could not load XML document {}", fullPath);
            da.addXmlErrorLog(xmlFile, "Could not load XML document '" + fullPath + "'");
        } catch (SAXParseException e) {
            logger.error(e.getMessage());
            da.addXmlErrorLog(xmlFile, e);
        } catch (SAXException e) {
            logger.error(e.getMessage());
            da.addXmlErrorLog(xmlFile, e.getMessage());
        }
        return returnValue;
    }

    private boolean validateProcedureResults() {
        boolean returnValue = false;
        try {
            ProcedureResultsXMLValidityChecker validator = new ProcedureResultsXMLValidityChecker();
            validator.attachErrorHandler();
            validator.check(fullPath);
            if (validator.errorsFound()) {
                logger.debug("Procedure XML document '{}' contains errors", fullPath);
                List<SAXParseException> issues = validator.getHandler().getExceptionMessages();
                if (issues != null && !issues.isEmpty()) {
                    da.addXmlErrorLogs(xmlFile, issues);
                }
            } else {
                returnValue = true;
            }
        } catch (IOException e) {
            logger.error("Could not load XML document {}", fullPath);
            da.addXmlErrorLog(xmlFile, "Could not load XML document '" + fullPath + "'");
        } catch (SAXParseException e) {
            logger.error(e.getMessage());
            da.addXmlErrorLog(xmlFile, e);
        } catch (SAXException e) {
            logger.error(e.getMessage());
            da.addXmlErrorLog(xmlFile, e.getMessage());
        }
        return returnValue;
    }
}
