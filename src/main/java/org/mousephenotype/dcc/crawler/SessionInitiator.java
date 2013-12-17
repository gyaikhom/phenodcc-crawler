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

import java.util.TimerTask;
import org.mousephenotype.dcc.crawler.entities.CrawlingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Every crawling exercise happens inside a unique crawling session. We use
 * sessions to track individual characteristics of the crawling and processing
 * phases. The data collected for each of these sessions may be used later to
 * improve the performance of the crawler.
 * 
 * @author Gagarine Yaikhom <g.yaikhom@har.mrc.ac.uk>
 */
public class SessionInitiator extends TimerTask {

    private final Logger logger = LoggerFactory.getLogger(SessionInitiator.class);
    private int numDownloaders;
    private int poolSize;
    private int maxRetries;
    private String backupDir;
    private boolean isActive;

    public SessionInitiator(String backupDir, int numDownloaders, int poolSize, int maxRetries) {
        this.backupDir = backupDir;
        this.numDownloaders = numDownloaders;
        this.poolSize = poolSize;
        this.maxRetries = maxRetries;
        this.isActive = false;
    }

    @Override
    public void run() {
        if (isActive) {
            return;
        }
        DatabaseAccessor da = DatabaseAccessor.getInstance();
        CrawlingSession session = da.beginCrawling();
                
        logger.debug("Starting download manager");
        isActive = true;
        DownloadManager dm = new DownloadManager(backupDir, numDownloaders, poolSize, maxRetries);
        Thread t = new Thread(dm);
        t.start();
        try {
            logger.debug("Waiting for download manager to finish");
            t.join();
            logger.debug("Download manager has finished");
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }

        DataInserter di = DataInserter.getInstance();
        short status = di.updateQCDatabase(backupDir, session);
        da.finishCrawling(session, status);

        da.closeEntityManagerFactory();
        isActive = false;
        logger.info("All done...");
    }
}
