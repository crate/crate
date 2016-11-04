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
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.BytesRefs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class ExtendedFsStats implements Iterable<ExtendedFsStats.Info>, Streamable {

    public static class Info implements Streamable {

        private BytesRef path;
        @Nullable
        private BytesRef dev;
        private long total = -1;
        private long free = -1;
        private long available = -1;
        private long used = -1;
        private long diskReads = -1;
        private long diskWrites = -1;
        private long diskReadBytes = -1;
        private long diskWriteBytes = -1;

        public static Info readInfo(StreamInput in) throws IOException {
            Info info = new Info();
            info.readFrom(in);
            return info;
        }

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

        public void path(String path) {
            this.path = BytesRefs.toBytesRef(path);
        }

        @Nullable
        public BytesRef dev() {
            return dev;
        }

        public void dev(@Nullable String dev) {
            this.dev = BytesRefs.toBytesRef(dev);
        }

        public long total() {
            return total;
        }

        public void total(long total) {
            this.total = total;
        }

        public long free() {
            return free;
        }

        public void free(long free) {
            this.free = free;
        }

        public long available() {
            return available;
        }

        public void available(long available) {
            this.available = available;
        }

        public long used() {
            return used;
        }

        public void used(long used) {
            this.used = used;
        }

        public long diskReads() {
            return this.diskReads;
        }

        public void diskReads(long diskReads) {
            this.diskReads = diskReads;
        }

        public long diskWrites() {
            return this.diskWrites;
        }

        public void diskWrites(long diskWrites) {
            this.diskWrites = diskWrites;
        }

        public long diskReadSizeInBytes() {
            return diskReadBytes;
        }

        public void diskReadSizeInBytes(long diskReadBytes) {
            this.diskReadBytes = diskReadBytes;
        }

        public long diskWriteSizeInBytes() {
            return diskWriteBytes;
        }

        public void diskWriteSizeInBytes(long diskWriteBytes) {
            this.diskWriteBytes = diskWriteBytes;
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

        @Override
        public void readFrom(StreamInput in) throws IOException {
            total = in.readLong();
            free = in.readLong();
            available = in.readLong();
            used = in.readLong();
            diskReads = in.readLong();
            diskWrites = in.readLong();
            diskReadBytes = in.readLong();
            diskWriteBytes = in.readLong();
            path = DataTypes.STRING.readValueFrom(in);
            dev = DataTypes.STRING.readValueFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(total);
            out.writeLong(free);
            out.writeLong(available);
            out.writeLong(used);
            out.writeLong(diskReads);
            out.writeLong(diskWrites);
            out.writeLong(diskReadBytes);
            out.writeLong(diskWriteBytes);
            DataTypes.STRING.writeValueTo(out, path);
            DataTypes.STRING.writeValueTo(out, dev);
        }
    }

    private Info[] infos = new Info[0];
    private Info total;

    public ExtendedFsStats() {
    }

    public ExtendedFsStats(Info total) {
        this.total = total;
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

    public void total(Info total) {
        this.total = total;
    }

    public Info[] infos() {
        return infos;
    }

    public void infos(Info[] infos) {
        this.infos = infos;
    }

    @Override
    public Iterator<Info> iterator() {
        return Iterators.forArray(infos);
    }

    public int size() {
        return infos.length;
    }

    public static ExtendedFsStats readExtendedFsStats(StreamInput in) throws IOException {
        ExtendedFsStats stat = new ExtendedFsStats();
        stat.readFrom(in);
        return stat;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        total = in.readOptionalStreamable(Info::new);
        infos = new Info[in.readVInt()];
        for (int i = 0; i < infos.length; i++) {
            infos[i] = Info.readInfo(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalStreamable(total);
        out.writeVInt(infos.length);
        for (Info info : infos) {
            info.writeTo(out);
        }
    }
}
