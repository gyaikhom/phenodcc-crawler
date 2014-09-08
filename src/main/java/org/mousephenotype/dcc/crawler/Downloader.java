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

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.mousephenotype.dcc.crawler.entities.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An instance of the downloader is responsible for downloading a specific zip
 * file. Multiple downloaders are created by the DownloadManager, the
 * multiplicity of which is determined by the command line options supplied by
 * the user. A downloader will continue to download files as long as there are
 * files to be downloaded. This decision is based on the file download map
 * created by the crawlers (FtpCrawler or SftpCrawler). When no files are left
 * to download, the downloader instances return to DownloadManager.
 *
 * Since every Zip file that has been downloaded successfully can be processed
 * simultaneously, each downloader spawns a processing thread for each
 * downloaded zip file. This thread runs an instance of XmlExtractor.
 *
 * @author Gagarine Yaikhom <g.yaikhom@har.mrc.ac.uk>
 */
public class Downloader implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(Downloader.class);
    private String hostname;
    private String protocol;

    /* part relevant to ftp download */
    private FTPClient client;
    private Map<String, FTPClient> ftpConnections;

    /* part relevant to sftp download */
    private Session session;
    private ChannelSftp channel;
    private Map<String, Session> sftpSessions;
    private Map<String, ChannelSftp> sftpChannels;

    /* downloader multithreading */
    private int poolSize;
    private int maxRetries;
    private String backupDir;
    private DatabaseAccessor da;

    private final int CONNECT_TIMEOUT_MILLISECS = 300000; // 5 minutes

    Downloader(String backupDir, int poolSize, int maxRetries) {
        ftpConnections = new HashMap<>();
        sftpSessions = new HashMap<>();
        sftpChannels = new HashMap<>();
        this.backupDir = backupDir;
        this.poolSize = poolSize;
        this.maxRetries = maxRetries;
    }

    @Override
    public void run() {
        da = DatabaseAccessor.getInstance();
        try {
            downloadFiles();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        } finally {
            closeOpenConnections();
        }
    }

    private void downloadFiles() throws InterruptedException {
        ExecutorService workers = Executors.newFixedThreadPool(poolSize);
        while (true) {
            SortedSet<FileSourceHasZip> f = getActionAndSources();
            if (f == null || f.isEmpty()) {
                logger.info("No download pending; downloader will now exit");
                break;
            } else {
                ZipDownload zd = attemptDownload(f);
                if (zd != null) {
                    logger.debug("Successfully downloaded '{}'... will now extract contents",
                            zd.getZfId().getZaId().getZipId().getFileName());
                    workers.submit(new XmlExtractor(backupDir, zd));
                }
            }
        }
        workers.shutdown();
        workers.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    private SortedSet<FileSourceHasZip> getActionAndSources() {
        SortedSet<FileSourceHasZip> returnValue = null;
        Phase p = da.getPhase(Phase.CHECK_ZIP_FILENAME);
        AStatus s = da.getStatus(AStatus.DONE);
        ZipAction zipAction = null;

        // try to take a download job
        while (true) {
            zipAction = da.getZipAction(p, s);
            if (zipAction == null) {
                logger.debug("No file to download.");
                break; // no more download jobs left
            } else {
                if (da.takeDownloadJob(zipAction)) {
                    logger.debug("Will now process zip action '{}' for zip file '{}'.",
                            zipAction.getTodoId().getShortName(),
                            zipAction.getZipId().getFileName());
                    break; // got a download job
                } else {
                    logger.debug("Zip action '{}' for zip file '{}' is already handled.",
                            zipAction.getTodoId().getShortName(),
                            zipAction.getZipId().getFileName());
                    continue; // download job has been taken; try again
                }
            }
        }

        // did we get a download job?
        if (zipAction != null) {
            List<FileSourceHasZip> zf = da.getFileSourceHasZipCollection(zipAction);
            if (zf != null && !zf.isEmpty()) {
                returnValue = sortByPreference(zf);
            } else {
                logger.error("None of the file sources host the zip file '{}' with action '{}'",
                        zipAction.getZipId().getFileName(),
                        zipAction.getTodoId().getShortName());
            }
        }
        return returnValue;
    }

    private SortedSet<FileSourceHasZip> sortByPreference(Collection<FileSourceHasZip> zf) {
        SortedSet<FileSourceHasZip> s = new TreeSet<>(new FileSourcePreferenceComparator());
        Iterator<FileSourceHasZip> i = zf.iterator();
        if (i.hasNext()) {
            FileSourceHasZip z = i.next();
            z.setRating((short) FileSourceRatingCalculator.calculate(z));
            s.add(z);
        }
        return s;
    }

    private ZipDownload attemptDownload(SortedSet<FileSourceHasZip> f) {
        ZipDownload zd = null;
        Iterator<FileSourceHasZip> fi = f.iterator();

        // try to download from all file source servers hosting the file
        while (fi.hasNext()) {
            FileSourceHasZip fileSourceHasZip = fi.next();
            if (prepareDownload(fileSourceHasZip)) {
                int remainingAttempts = maxRetries;
                while (remainingAttempts-- > 0) {
                    zd = download(fileSourceHasZip);
                    if (zd != null) {
                        break; // download completed: avoid re-downloading
                    }
                }
                if (zd != null) {
                    break; // download completed: don't try remaining sources
                }
            } else {
                logger.warn("Could not prepare connection for downloading '{}' from '{}'",
                        fileSourceHasZip.getZaId().getZipId().getFileName(),
                        fileSourceHasZip.getFileSourceId().getHostname());
            }
        }
        return zd;
    }

    private boolean prepareDownload(FileSourceHasZip f) {
        boolean returnValue = false;
        FileSource fs = f.getFileSourceId();
        if (fs != null && fs.getHostname() != null) {
            returnValue = prepareConnection(fs);
        } else {
            logger.error("Invalid file source, or no valid file source hostname to use");
        }
        return returnValue;
    }

    private boolean prepareConnection(FileSource fs) {
        boolean returnValue = false;
        hostname = fs.getHostname();
        protocol = fs.getProtocolId().getShortName();
        switch (protocol) {
            case "ftp":
                if (ftpConnections.containsKey(hostname)) {
                    client = ftpConnections.get(hostname);
                    if (client.isConnected() && client.isAvailable()) {
                        returnValue = true; // reuse existing connection
                    }
                }
                break;
            case "sftp":
                if (sftpSessions.containsKey(hostname)) {
                    session = sftpSessions.get(hostname);
                    if (session.isConnected()) {
                        if (sftpChannels.containsKey(hostname)) {
                            channel = sftpChannels.get(hostname);
                            if (channel.isConnected()) {
                                returnValue = true; // reuse existing connection
                            }
                        }
                    }
                }
                break;
        }

        // if no existing connection, create a new one
        if (!returnValue) {
            returnValue = establishNewConnection(fs);
        }
        return returnValue;
    }

    private boolean establishNewConnection(FileSource fs) {
        boolean returnValue = false;
        String username, password;

        username = fs.getUsername();
        password = fs.getAccesskey();

        if (username == null || username.isEmpty()) {
            logger.error("Invalid username for server '{}'", hostname);
        } else {
            switch (protocol) {
                case "ftp":
                    if (password == null || password.isEmpty()) {
                        logger.error("Invalid password for server '{}'", hostname);
                    } else {
                        if (ftpConnect(username, password)) {
                            // save for later reuse
                            ftpConnections.put(hostname, client);
                            returnValue = true;
                        }
                    }
                    break;
                case "sftp":
                    // If password is empty, try public key authentication.
                    if (sftpConnect(username, password)) {
                        // save for later reuse
                        sftpSessions.put(hostname, session);
                        sftpChannels.put(hostname, channel);
                        returnValue = true;
                    }
                    break;
                default:
                    logger.error("Downloader currently does not support '{}' protocol", protocol);
                    break;
            }
        }
        return returnValue;
    }

    private boolean sftpConnect(String username, String password) {
        boolean returnValue = false;
        try {
            logger.debug("Downloader is trying to connect to '{}'", hostname);
            JSch jsch = new JSch();
            session = jsch.getSession(username, hostname);

            /* if password is null or empty, assume public key
             authentication. This facility was added to handle RIKEN file
             server. Ensure that the key have been added to the .ssh/
             dirctories. */
            Properties config = new Properties();
            if (password == null || password.isEmpty()) {
                jsch.setKnownHosts("~/.ssh/known_hosts");
                jsch.addIdentity("~/.ssh/id_rsa");
                config.put("PreferredAuthentications", "publickey");
            } else {
                session.setPassword(password);
            }
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect(CONNECT_TIMEOUT_MILLISECS);

            if (session.isConnected()) {
                logger.debug("Downloader now has a valid session with sftp server at '{}'", hostname);
                Channel temp = session.openChannel("sftp");
                temp.connect();
                channel = (ChannelSftp) temp;
                if (channel.isConnected()) {
                    logger.debug("Downloader has opened a channel with sftp server at '{}'", hostname);
                    returnValue = true;
                } else {
                    logger.debug("Downloader could not open a channel with sftp server at '{}'", hostname);
                    if (session.isConnected()) {
                        session.disconnect();
                    }
                }
            } else {
                logger.error("Failed to establish a session with sftp server at '{}'", hostname);
            }
        } catch (JSchException e) {
            logger.error(e.getMessage());
        }

        return returnValue;
    }

    private boolean ftpConnect(String username, String password) {
        boolean returnValue = false;
        try {
            client = new FTPClient();
            client.connect(hostname);
            client.login(username, password);
            int reply = client.getReplyCode();
            if (FTPReply.isPositiveCompletion(reply)) {
                if (client.setFileType(FTPClient.BINARY_FILE_TYPE)) {
                    returnValue = true;
                } else {
                    logger.error("Failed to set ftp transfer mode to binary for '{}'... will now close connection", hostname);
                    client.disconnect();
                }
            } else {
                logger.error("Server at '{}' refused connection", hostname);
                client.disconnect();
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return returnValue;
    }

    private ZipDownload download(FileSourceHasZip ftpHasZip) {
        ZipDownload zipDownload = da.downloadBegins(ftpHasZip);
        ZipAction zipAction = ftpHasZip.getZaId();
        final ZipFile zipFile = zipAction.getZipId();
        final String fn = zipFile.getFileName();
        final String srcPath = ftpHasZip.getFileSourceId().getBasePath()
                + zipAction.getTodoId().getShortName() + "/" + fn;
        final String destPath = backupDir + "/"
                + zipAction.getTodoId().getShortName() + "/" + fn;
        FileOutputStream fos = null;
        boolean downloadSuccess = false;

        try {
            fos = new FileOutputStream(destPath);
            DownloadTracker p = new DownloadTracker(zipDownload,
                    fos, zipFile.getSizeBytes());
            switch (protocol) {
                case "ftp":
                    try {
                        downloadSuccess = client.retrieveFile(srcPath, p);
                    } catch (IOException e) {
                        logger.error(e.getMessage());
                    }
                    break;
                case "sftp":
                    try {
                        channel.get(srcPath, p);
                        downloadSuccess = true;
                    } catch (SftpException e) {
                        logger.error(e.getMessage());
                    }
                    break;
            }
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage());
        } finally {
            try {
                if (fos != null) {
                    fos.close();
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        if (downloadSuccess) {
            logger.debug("Successfully downloaded '{}' with action '{}'",
                    fn, zipAction.getTodoId().getShortName());
            da.downloadDone(zipDownload);
        } else {
            da.downloadFailed(zipDownload);
            logger.error("Failed to download '{}' with action '{}'",
                    fn, zipAction.getTodoId().getShortName());
            zipDownload = null;
        }
        return zipDownload;
    }

    private void closeOpenConnections() {
        // close all ftp connections
        Iterator<Entry<String, FTPClient>> ci = ftpConnections.entrySet().iterator();
        while (ci.hasNext()) {
            Entry<String, FTPClient> item = ci.next();
            FTPClient c = item.getValue();
            if (c.isConnected()) {
                try {
                    c.logout();
                    c.disconnect();
                    logger.debug("Downloader has disconnected from '{}'", item.getKey());
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }
            ci.remove();
        }

        // close all sftp channels (before closing sessions)
        Iterator<Entry<String, ChannelSftp>> ch = sftpChannels.entrySet().iterator();
        while (ch.hasNext()) {
            Entry<String, ChannelSftp> item = ch.next();
            ChannelSftp c = item.getValue();
            if (c.isConnected()) {
                c.disconnect();
                logger.debug("Downloader has closed sftp channel with '{}'", item.getKey());
            }
            ch.remove();
        }

        // close all sftp sessions
        Iterator<Entry<String, Session>> se = sftpSessions.entrySet().iterator();
        while (se.hasNext()) {
            Entry<String, Session> item = se.next();
            Session c = item.getValue();
            if (c.isConnected()) {
                c.disconnect();
                logger.debug("Downloader has closed sftp session with '{}'", item.getKey());
            }
            se.remove();
        }
    }
}
