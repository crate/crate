/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.monitor;

import com.google.common.collect.Iterators;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class ExtendedFsStats implements Iterable<ExtendedFsStats.Info> {

    public static class Info {

        BytesRef path;
        @Nullable
        BytesRef dev;
        long total = -1;
        long free = -1;
        long available = -1;
        long used = -1;
        long diskReads = -1;
        long diskWrites = -1;
        long diskReadBytes = -1;
        long diskWriteBytes = -1;

        public Info() {
        }

        public Info(String path,
                    @Nullable String dev,
                    long total,
                    long free,
                    long available,
                    long used,
                    long diskReads,
                    long diskWrites,
                    long diskReadBytes,
                    long diskWriteBytes) {
            this.path = new BytesRef(path);
            this.dev = BytesRefs.toBytesRef(dev);
            this.total = total;
            this.free = free;
            this.available = available;
            this.used = used;
            this.diskReads = diskReads;
            this.diskWrites = diskWrites;
            this.diskReadBytes = diskReadBytes;
            this.diskWriteBytes = diskWriteBytes;
        }

        public BytesRef path() {
            return path;
        }

        @Nullable
        public BytesRef dev() {
            return dev;
        }

        public long total() {
            return total;
        }

        public long free() {
            return free;
        }

        public long available() {
            return available;
        }

        public long used() {
            return used;
        }

        public long diskReads() {
            return this.diskReads;
        }

        public long diskWrites() {
            return this.diskWrites;
        }

        public long diskReadSizeInBytes() {
            return diskReadBytes;
        }

        public long diskWriteSizeInBytes() {
            return diskWriteBytes;
        }

        public void add(Info info) {
            total = addLong(total, info.total);
            free = addLong(free, info.free);
            available = addLong(available, info.available);
            used = addLong(used, info.used);
            diskReads = addLong(diskReads, info.diskReads);
            diskWrites = addLong(diskWrites, info.diskWrites);
            diskReadBytes = addLong(diskReadBytes, info.diskReadBytes);
            diskWriteBytes = addLong(diskWriteBytes, info.diskWriteBytes);
        }

        private long addLong(long existing, long added) {
            if (existing < 0) {
                return added;
            }
            return existing + added;
        }

    }

    final Info[] infos;
    Info total;

    public ExtendedFsStats(Info total) {
        this.total = total;
        this.infos = new Info[0];
    }

    public ExtendedFsStats(Info[] infos) {
        this.infos = infos;
        this.total = null;
    }

    public Info total() {
        if (total != null) {
            return total;
        }
        Info res = new Info();
        Set<BytesRef> seenDevices = new HashSet<>(infos.length);
        for (Info subInfo : infos) {
            if (subInfo.dev != null) {
                if (!seenDevices.add(subInfo.dev)) {
                    continue; // already added numbers for this device;
                }
            }
            res.add(subInfo);
        }
        total = res;
        return res;
    }

    @Override
    public Iterator<Info> iterator() {
        return Iterators.forArray(infos);
    }

    public int size() {
        return infos.length;
    }
}
