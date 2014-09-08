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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SingleInstance is a singleton class that is used to prevent multiple
 * crawlers from running simultaneously. Because the crawler is multi-threaded
 * and updates the phenodcc_tracker database, we must ensure that no two
 * crawler instances that are targetting the same tracker database are running
 * at the same moment. To detect this, we use a file locking mechanism.
 * 
 * NOTE: This will not prevent multiple runs if the crawler is executed from
 * multiple directories. But this is outside our control anyway.
 *
 * @author Gagarine Yaikhom <g.yaikhom@har.mrc.ac.uk>
 */
public class SingleInstance {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleInstance.class);
    public static final String LOCK_PATH = "phenodcc.lock";
    public static final int NOT_RUNNING = 0;
    public static final int ALREADY_RUNNING = 1;
    public static final int INVALID_LOCK = 2;
    private static File lockFile = null;
    private static FileChannel fileChannel = null;
    private static FileLock fileLock = null;
    private static SingleInstance instance = null;

    protected SingleInstance() {
    }

    public static SingleInstance getInstance() {
        if (instance == null) {
            instance = new SingleInstance();
        }
        return instance;
    }

    public String getLockPath() {
        return LOCK_PATH;
    }
    
    /**
     * Method check() checks if the application is already running.
     *
     * The method first checks if the lock path exists. If it does not, no
     * instance is currently running. If the lock path exists, we check if it is
     * a file. If it is not, the lock path is corrupt. The administrator must
     * fix this by verifying and deleting the existing path. If the lock path is
     * a file, we try to get a read/write lock on that file. If we could not get
     * a lock, it means that another instance is already running. If any I/O
     * exception occurs, we must assume that the lock path is corrupt.
     *
     * @return An integer which gives the current status. 0 for no instance
     * running; 1 for an instance already running; and 2 for corrupt lock path.
     */
    public int check() {
        int status = NOT_RUNNING; // assume that no instance is running
        
        // check if the file already exists, and that it is not a directory.
        // if the file does not exists, create a new file.
        lockFile = new File(LOCK_PATH);
        if (lockFile.exists()) {
            if (!lockFile.isFile()) {
                LOGGER.error("The lock path '{}' must be a file, not a directory", LOCK_PATH);
                status = INVALID_LOCK;
            }
        } else {
            try {
                lockFile.createNewFile();
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
                status = INVALID_LOCK;
            }
        }

        // if the existing lock is valid, or a new lock file was created
        if (status != INVALID_LOCK) {
            try {
                RandomAccessFile raf = new RandomAccessFile(lockFile, "rw");
                fileChannel = raf.getChannel();
                try {
                    fileLock = fileChannel.tryLock();
                    if (fileLock == null) {
                        fileChannel.close();
                        status = ALREADY_RUNNING;
                    }
                } catch (IOException e) {
                    LOGGER.error(e.getMessage());
                    status = INVALID_LOCK;
                }
            } catch (FileNotFoundException e) {
                LOGGER.error("The impossible scenario has happened!");
                status = INVALID_LOCK;
            }
        }

        return status;
    }

    /**
     * Method shutdown() clears the file lock and deletes the lock path.
     */
    public void shutdown() {
        if (fileLock != null) {
            try {
                fileLock.release();
                fileChannel.close();
                lockFile.delete();
                fileLock = null;
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
            }
        }
    }
}