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

package org.elasticsearch.index.seqno;

import java.io.IOException;
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

/**
 * Represents retention lease stats.
 */
public final class RetentionLeaseStats implements Writeable {

    private final RetentionLeases leases;

    /**
     * The underlying retention leases backing this stats object.
     *
     * @return the leases
     */
    public RetentionLeases leases() {
        return leases;
    }

    /**
     * Constructs a new retention lease stats object from the specified leases.
     *
     * @param leases the leases
     */
    public RetentionLeaseStats(final RetentionLeases leases) {
        this.leases = Objects.requireNonNull(leases);
    }

    /**
     * Constructs a new retention lease stats object from a stream. The retention lease stats should have been written via
     * {@link #writeTo(StreamOutput)}.
     *
     * @param in the stream to construct the retention lease stats from
     * @throws IOException if an I/O exception occurs reading from the stream
     */
    public RetentionLeaseStats(final StreamInput in) throws IOException {
        leases = new RetentionLeases(in);
    }

    /**
     * Writes a retention lease stats object to a stream in a manner suitable for later reconstruction via
     * {@link #RetentionLeaseStats(StreamInput)} (StreamInput)}.
     *
     * @param out the stream to write the retention lease stats to
     * @throws IOException if an I/O exception occurs writing to the stream
     */
    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        leases.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final RetentionLeaseStats that = (RetentionLeaseStats) o;
        return Objects.equals(leases, that.leases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leases);
    }
}
