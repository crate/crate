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

package io.crate.statistics;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

public record Stats(long numDocs, long sizeInBytes) implements Writeable {

    public static final Stats EMPTY = new Stats(-1, -1);

    public static Stats readFrom(StreamInput in) throws IOException {
        long numDocs = in.readLong();
        long sizeInBytes = in.readLong();
        return new Stats(numDocs, sizeInBytes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(numDocs);
        out.writeLong(sizeInBytes);
    }

    public boolean isEmpty() {
        return numDocs == -1 && sizeInBytes == -1;
    }

    public Stats withNumDocs(long numDocs) {
        long sizePerRow = averageSizePerRowInBytes();
        if (sizePerRow < 1) {
            return new Stats(numDocs, -1);
        } else {
            return new Stats(numDocs, sizePerRow * numDocs);
        }
    }

    public Stats add(Stats other) {
        return new Stats(
            numDocs == -1 || other.numDocs == -1
                ? -1
                : numDocs + other.numDocs,
            sizeInBytes == -1 || other.sizeInBytes == -1
                ? -1
                : sizeInBytes + other.sizeInBytes
        );
    }

    public long numDocs() {
        return numDocs;
    }

    public long sizeInBytes() {
        return sizeInBytes;
    }

    public long averageSizePerRowInBytes() {
        if (numDocs == -1) {
            return -1;
        } else if (numDocs == 0) {
            return 0;
        } else {
            return sizeInBytes / numDocs;
        }
    }
}
