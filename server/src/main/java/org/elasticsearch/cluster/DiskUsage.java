/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster;

import java.io.IOException;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;

/**
 * Represents the amount of disk used on a node.
 */
public record DiskUsage(String nodeId,
                        String nodeName,
                        String path,
                        long totalBytes,
                        long freeBytes) implements Writeable {

    public static DiskUsage of(StreamInput in) throws IOException {
        String nodeId = in.readString();
        String nodeName = in.readString();
        String path = in.readString();
        long totalBytes = in.readVLong();
        long freeBytes = in.readVLong();
        return new DiskUsage(nodeId, nodeName, path, totalBytes, freeBytes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.nodeId);
        out.writeString(this.nodeName);
        out.writeString(this.path);
        out.writeVLong(this.totalBytes);
        out.writeVLong(this.freeBytes);
    }


    /// @return Free disk percentage. 100% if [totalBytes] is 0.
    public double getFreeDiskAsPercentage() {
        if (totalBytes == 0) {
            return 100.0;
        }
        return 100.0 * ((double)freeBytes / totalBytes);
    }

    public double getUsedDiskAsPercentage() {
        return 100.0 - getFreeDiskAsPercentage();
    }

    public long getUsedBytes() {
        return totalBytes() - freeBytes();
    }

    @Override
    public String toString() {
        return "[" + nodeId + "][" + nodeName + "][" + path + "] free: " + new ByteSizeValue(freeBytes()) +
                "[" + Strings.format1Decimals(getFreeDiskAsPercentage(), "%") + "]";
    }
}
