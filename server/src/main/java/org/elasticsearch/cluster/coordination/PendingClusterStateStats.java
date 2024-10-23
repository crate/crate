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

package org.elasticsearch.cluster.coordination;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

/**
 * Class encapsulating stats about the PendingClusterStatsQueue
 */
public class PendingClusterStateStats implements Writeable {

    private final int total;
    private final int pending;
    private final int committed;

    public PendingClusterStateStats(int total, int pending, int committed) {
        this.total = total;
        this.pending = pending;
        this.committed = committed;
    }

    public PendingClusterStateStats(StreamInput in) throws IOException {
        total = in.readVInt();
        pending = in.readVInt();
        committed = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(total);
        out.writeVInt(pending);
        out.writeVInt(committed);
    }

    public int getCommitted() {
        return committed;
    }

    public int getPending() {
        return pending;
    }

    public int getTotal() {
        return total;
    }

    @Override
    public String toString() {
        return "PendingClusterStateStats(total=" + total + ", pending=" + pending + ", committed=" + committed + ")";
    }
}
