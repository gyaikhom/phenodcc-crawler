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

import java.io.FileOutputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.mousephenotype.dcc.crawler.entities.ZipDownload;

/**
 * The download tracker regularly updates the number of bytes that have
 * been downloaded so far. This may be used by the tracker web services to
 * check the latest file download status.
 *
 * @author Gagarine Yaikhom <g.yaikhom@har.mrc.ac.uk>
 */
public class DownloadTracker extends CountingOutputStream {

    private static final long MEGABYTES_TO_BYTES = 1048576L;
    private long sizeInBytes; // total size of the file in bytes
    private long doneInMegabytes; // already downloaded size in megabytes 
    private ZipDownload zipDownload;
    private static final double PERCENTAGE = 100.0;

    DownloadTracker(ZipDownload zipDownload, FileOutputStream fos, long sizeInBytes) {
        super(fos);
        this.sizeInBytes = sizeInBytes;
        this.doneInMegabytes = 0L;
        this.zipDownload = zipDownload;
    }

    @Override
    protected void beforeWrite(int n) {
        super.beforeWrite(n);

        // only update download progress into the database for every megabyte,
        // or when the download is complete
        long byteCount = getByteCount();
        if (byteCount == sizeInBytes
                || (byteCount / MEGABYTES_TO_BYTES) > doneInMegabytes) {
            ++doneInMegabytes;
            DatabaseAccessor da = DatabaseAccessor.getInstance();
            da.setDownloadProgress(zipDownload, getByteCount());
        }
    }

    public long getFileSize() {
        return sizeInBytes;
    }

    public float getDownloadPercentage() {
        return (float) ((getByteCount() * PERCENTAGE) / sizeInBytes);
    }
}
