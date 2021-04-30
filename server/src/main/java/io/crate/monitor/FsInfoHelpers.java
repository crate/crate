/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.monitor;

import org.elasticsearch.monitor.fs.FsInfo;

public class FsInfoHelpers {

    public static class Path {

        public static String dev(FsInfo.Path path) {
            return path.getMount() == null ? "" : path.getMount();
        }

        public static Long size(FsInfo.Path path) {
            return path.getTotal().getBytes();
        }

        public static Long used(FsInfo.Path path) {
            return path.getTotal().getBytes() == -1L || path.getAvailable().getBytes() == -1L ? -1L : path.getTotal().getBytes() - path.getAvailable().getBytes();
        }

        public static Long available(FsInfo.Path path) {
            return path.getAvailable().getBytes();
        }
    }

    public static class Stats {

        public static Long readOperations(FsInfo.IoStats ioStats) {
            if (ioStats != null) {
                return ioStats.getTotalReadOperations();
            }
            return -1L;
        }

        public static Long bytesRead(FsInfo.IoStats ioStats) {
            if (ioStats != null) {
                return ioStats.getTotalReadKilobytes() * 1024L;
            }
            return -1L;
        }

        public static Long writeOperations(FsInfo.IoStats ioStats) {
            if (ioStats != null) {
                return ioStats.getTotalWriteOperations();
            }
            return -1L;
        }

        public static Long bytesWritten(FsInfo.IoStats ioStats) {
            if (ioStats != null) {
                return ioStats.getTotalWriteKilobytes() * 1024L;
            }
            return -1L;
        }
    }
}
