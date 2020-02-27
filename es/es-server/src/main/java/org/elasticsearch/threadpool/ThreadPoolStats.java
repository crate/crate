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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.AbstractList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ThreadPoolStats extends AbstractList<ThreadPoolStats.Stats> implements Writeable {

    public static class Stats implements Writeable, Comparable<Stats> {

        private final String name;
        private final int threads;
        private final int queue;
        private final int active;
        private final long rejected;
        private final int largest;
        private final long completed;

        public Stats(String name, int threads, int queue, int active, long rejected, int largest, long completed) {
            this.name = name;
            this.threads = threads;
            this.queue = queue;
            this.active = active;
            this.rejected = rejected;
            this.largest = largest;
            this.completed = completed;
        }

        public Stats(StreamInput in) throws IOException {
            name = in.readString();
            threads = in.readInt();
            queue = in.readInt();
            active = in.readInt();
            rejected = in.readLong();
            largest = in.readInt();
            completed = in.readLong();
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

        public String getName() {
            return this.name;
        }

        public int getThreads() {
            return this.threads;
        }

        public int getQueue() {
            return this.queue;
        }

        public int getActive() {
            return this.active;
        }

        public long getRejected() {
            return rejected;
        }

        public int getLargest() {
            return largest;
        }

        public long getCompleted() {
            return this.completed;
        }

        @Override
        public int compareTo(Stats other) {
            if ((getName() == null) && (other.getName() == null)) {
                return 0;
            } else if ((getName() != null) && (other.getName() == null)) {
                return 1;
            } else if (getName() == null) {
                return -1;
            } else {
                int compare = getName().compareTo(other.getName());
                if (compare == 0) {
                    compare = Integer.compare(getThreads(), other.getThreads());
                }
                return compare;
            }
        }
    }

    private List<Stats> stats;

    public ThreadPoolStats(List<Stats> stats) {
        Collections.sort(stats);
        this.stats = stats;
    }

    public ThreadPoolStats(StreamInput in) throws IOException {
        stats = in.readList(Stats::new);
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
    @Nonnull
    public Iterator<Stats> iterator() {
        return stats.iterator();
    }

    @Override
    public int size() {
        return stats.size();
    }
}
