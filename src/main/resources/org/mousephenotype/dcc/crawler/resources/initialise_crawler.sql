/******************************************************************************
 *                           The Tracker Database                             *
 *                                                                            *
 * DESCRIPTION:                                                               *
 * This MySQL script initialises the tracker database.                        *
 *                                                                            *
 * Copyright (c) 2012, The Medical Research Council Harwell                   *
 * Written by: G. Yaikhom (g.yaikhom@har.mrc.ac.uk)                           *
 *                                                                            *
 *****************************************************************************/

use phenodcc_tracker;

/******************************************************************************
 *                                                                            *
 * WARNING: This script will wipe the database clean.                    *
 *                                                                            *
 *****************************************************************************/
set foreign_key_checks = 0;
truncate a_status;
truncate an_exception;
truncate centre;
truncate crawling_session;
truncate file_source;
truncate file_source_has_zip;
truncate phase;
truncate processing_type;
truncate resource_state;
truncate session_task;
truncate source_protocol;
truncate xml_file;
truncate xml_log;
truncate zip_action;
truncate zip_download;
truncate zip_file;
truncate zip_log;
set foreign_key_checks = 1;

insert into centre (short_name, imits_name, full_name, address, email, phone, is_active, created) values
       ('Bcm', '', 'Baylor College of Medicine', '', '', '', true, now()),
       ('Gmc', '', 'Helmholtz Zentrum München', '', '', '', true, now()),
       ('H', 'HARWELL', 'MRC Harwell', 'Harwell Science and Innovation Campus, OX11 0RD', '', '', true, now()),
       ('Ics', 'ICS', 'Institut Clinique de la Souris', '', '', '', true, now()),
       ('J', 'JAX', 'The Jackson Laboratory', '', '', '', true, now()),
       ('Krb', '', 'Korea Research Institute of Bioscience & Biotechnology', '', '', '', true, now()),
       ('Ning', '', 'Nanjing University', '', '', '', true, now()),
       ('Rbrc', '', 'RIKEN Tsukuba Institute, BioResource Center', '', '', '', true, now()),
       ('Tcp', '', 'The Toronto Centre for Phenogenomics', '', '', '', true, now()),
       ('Ucd', '', 'University of California, Davis', '', '', '', true, now()),
       ('Wtsi', 'WTSI', 'Wellcome Trust Sanger Institute', 'Wellcome Trust Genome Campus, Hinxton, Cambridge CB10 1SA, UK', '', '', true, now());

insert into source_protocol (short_name, description) values
       ('ftp', 'File transfer protocol'),
       ('sftp', 'Secure file transfer protocol'),
       ('http', 'Hyper-text transfer protocol');

insert into resource_state (short_name, description) values
       ('available', 'Resource is up and running'),
       ('maintenance', 'Resource is temporarily unavailable for maintenance'),
       ('removed', 'Resource has been removed permanently');

insert into processing_type (short_name, description) values
       ('add', 'Add contents of file into the database'),
       ('edit', 'Update contents of the database with the contents of the file'),
       ('delete', 'Delete from the database entries that correspond to the contents of the file');

/* Order is important */
insert into phase (short_name, description) values
       ('download', 'Download file from one of the file data sources'),
       ('zip_name', 'Check if the name of the zipped data file conforms to convention'),
       ('zip_md5', 'Check Zip file integrity using MD5 check-sum'),
       ('unzip', 'Unzip the Zip file'),
       ('xml_name', 'Check if the name of the XML document conforms to convention'),
       ('xsd', 'Check validity of XML document against PhenoDCC XSD'),
       ('upload', 'Upload data from XML document to the QC database'),
       ('data', 'Check data integrity relative to QC database'),
       ('context', 'Builds the context tables for the tracker'),
       ('overview', 'Builds overview tables from the raw data just uploaded'),
       ('qc', 'Data is now ready for quality control');

insert into a_status (short_name, description, rgba) values
       ('pending', 'Phase is waiting to be run', 'aaaaaa00'),
       ('running', 'Phase is currently running', '0000ff00'),
       ('done', 'Phase has completed successfully', '00ff0000'),
       ('cancelled', 'Phase has been cancelled', '77007700'),
       ('failed', 'Terminated due to irrecoverable error', 'ff000000');
