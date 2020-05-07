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

package org.elasticsearch.index.translog;

public class TranslogStats {

    private final long translogSizeInBytes;
    private final int numberOfOperations;
    private final long uncommittedSizeInBytes;
    private final int uncommittedOperations;

    public TranslogStats(int numberOfOperations,
                         long translogSizeInBytes,
                         int uncommittedOperations,
                         long uncommittedSizeInBytes) {
        if (numberOfOperations < 0) {
            throw new IllegalArgumentException("numberOfOperations must be >= 0");
        }
        if (translogSizeInBytes < 0) {
            throw new IllegalArgumentException("translogSizeInBytes must be >= 0");
        }
        if (uncommittedOperations < 0) {
            throw new IllegalArgumentException("uncommittedOperations must be >= 0");
        }
        if (uncommittedSizeInBytes < 0) {
            throw new IllegalArgumentException("uncommittedSizeInBytes must be >= 0");
        }
        this.numberOfOperations = numberOfOperations;
        this.translogSizeInBytes = translogSizeInBytes;
        this.uncommittedSizeInBytes = uncommittedSizeInBytes;
        this.uncommittedOperations = uncommittedOperations;
    }

    public long getTranslogSizeInBytes() {
        return translogSizeInBytes;
    }

    public int estimatedNumberOfOperations() {
        return numberOfOperations;
    }

    /** the size of the generations in the translog that weren't yet to committed to lucene */
    public long getUncommittedSizeInBytes() {
        return uncommittedSizeInBytes;
    }

    /** the number of operations in generations of the translog that weren't yet to committed to lucene */
    public int getUncommittedOperations() {
        return uncommittedOperations;
    }

    @Override
    public String toString() {
        return "TranslogStats{" +
               "translogSizeInBytes=" + translogSizeInBytes +
               ", numberOfOperations=" + numberOfOperations +
               ", uncommittedSizeInBytes=" + uncommittedSizeInBytes +
               ", uncommittedOperations=" + uncommittedOperations +
               '}';
    }
}
