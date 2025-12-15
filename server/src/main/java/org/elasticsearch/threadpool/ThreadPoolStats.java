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

package org.elasticsearch.threadpool;

import java.io.IOException;
import java.util.AbstractList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

public class ThreadPoolStats extends AbstractList<ThreadPoolStats.Stats> implements Writeable {

    public static record Stats(String name,
                               int threads,
                               int queue,
                               int active,
                               long rejected,
                               int largest,
                               long completed) implements Writeable, Comparable<Stats> {

        public static Stats of(StreamInput in) throws IOException {
            String name = in.readString();
            int threads = in.readInt();
            int queue = in.readInt();
            int active = in.readInt();
            long rejected = in.readLong();
            int largest = in.readInt();
            long completed = in.readLong();
            return new Stats(name, threads, queue, active, rejected, largest, completed);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeInt(threads);
            out.writeInt(queue);
            out.writeInt(active);
            out.writeLong(rejected);
            out.writeInt(largest);
            out.writeLong(completed);
        }

        @Override
        public int compareTo(Stats other) {
            if ((name() == null) && (other.name() == null)) {
                return 0;
            } else if ((name() != null) && (other.name() == null)) {
                return 1;
            } else if (name() == null) {
                return -1;
            } else {
                int compare = name().compareTo(other.name());
                if (compare == 0) {
                    compare = Integer.compare(threads(), other.threads());
                }
                return compare;
            }
        }
    }

    private final List<Stats> stats;

    public ThreadPoolStats(List<Stats> stats) {
        Collections.sort(stats);
        this.stats = stats;
    }

    public ThreadPoolStats(StreamInput in) throws IOException {
        stats = in.readList(Stats::of);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(stats);
    }

    @Override
    public Stats get(int index) {
        return stats.get(index);
    }

    @Override
    public Iterator<Stats> iterator() {
        return stats.iterator();
    }

    @Override
    public int size() {
        return stats.size();
    }
}
