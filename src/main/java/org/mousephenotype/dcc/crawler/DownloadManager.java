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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.mousephenotype.dcc.crawler.entities.Centre;
import org.mousephenotype.dcc.crawler.entities.FileSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The download manager is responsible for running a multi-threaded crawling
 * session. When it is run, it first visits all of the FTP servers using
 * multi-threaded crawlers which creates a download map (which file to download
 * from where). This is then used by multi-threaded file downloaders to actually
 * download the files.
 * 
 * @author Gagarine Yaikhom <g.yaikhom@har.mrc.ac.uk>
 */
public class DownloadManager implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(DownloadManager.class);
    private int numDownloaders;
    private int poolSize;
    private int maxRetries;
    private String backupDir;
    private DatabaseAccessor da;

    DownloadManager(String backupDir, int numDownloaders, int poolSize, int maxRetries) {
        this.backupDir = backupDir;
        this.numDownloaders = numDownloaders;
        this.poolSize = poolSize;
        this.maxRetries = maxRetries;
    }

    @Override
    public void run() {
        da = DatabaseAccessor.getInstance();
        try {
            logger.debug("Will begin crawling");
            crawl();
            logger.debug("Will begin downloading and processing");
            download();
        } catch (InterruptedException e) {
        }
    }

    private void crawl() throws InterruptedException {
        List<Centre> cl = da.getCentres();
        if (cl == null) {
            return;
        }

        ExecutorService crawlers = Executors.newFixedThreadPool(poolSize);
        Iterator<Centre> ci = cl.iterator();
        while (ci.hasNext()) {
            Centre c = ci.next();
            List<FileSource> f = da.getFileSources(c);
            if (f == null || f.isEmpty()) {
                continue;
            }

            Iterator<FileSource> fi = f.iterator();
            while (fi.hasNext()) {
                FileSource fs = fi.next();
                logger.debug("Starting crawler for file source '{}' for centre '{}' ", fs.getHostname(), c.getShortName());
                String sourceType = fs.getProtocolId().getShortName();
                switch (sourceType) {
                    case "ftp":
                        crawlers.submit(new FtpCrawler(c, fs));
                        break;
                    case "sftp":
                        crawlers.submit(new SftpCrawler(c, fs));
                        break;
                    default:
                        logger.error("Crawler does not support the '{}' file transfer protocol", sourceType);
                        break;
                }
            }
        }
        crawlers.shutdown();
        crawlers.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    private void download() throws InterruptedException {
        ExecutorService downloaders = Executors.newFixedThreadPool(poolSize);
        int i = numDownloaders;
        while (i-- > 0) {
            logger.debug("Start downloader {}", i);
            downloaders.submit(new Downloader(backupDir, poolSize, maxRetries));
        }
        downloaders.shutdown();
        downloaders.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }
}
