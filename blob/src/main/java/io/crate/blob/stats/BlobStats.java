/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.blob.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class BlobStats implements Streamable {

    private long count;
    private long totalUsage;
    private String location;

    public String location() {
        return location;
    }

    public void location(String location) {
        this.location = location;
    }

    public long count() {
        return count;
    }

    public void count(long count) {
        this.count = count;
    }

    public long totalUsage() {
        return totalUsage;
    }

    public void totalUsage(long totalUsage) {
        this.totalUsage = totalUsage;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        count = in.readVLong();
        totalUsage = in.readVLong();
        location = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        out.writeVLong(totalUsage);
        out.writeString(location);
    }
}
