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

package org.cratedb.searchinto;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class WriterResult implements ToXContent, Streamable {

    private long totalWrites;
    private long failedWrites;
    private long succeededWrites;

    public void setTotalWrites(long totalWrites) {
        this.totalWrites = totalWrites;
    }

    public void setFailedWrites(long failedWrites) {
        this.failedWrites = failedWrites;
    }

    public void setSucceededWrites(long succeededWrites) {
        this.succeededWrites = succeededWrites;
    }

    public long getTotalWrites() {
        return totalWrites;
    }

    public long getFailedWrites() {
        return failedWrites;
    }

    public long getSucceededWrites() {
        return succeededWrites;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        totalWrites = in.readVLong();
        succeededWrites = in.readVLong();
        failedWrites = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalWrites);
        out.writeVLong(succeededWrites);
        out.writeVLong(failedWrites);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder,
            Params params) throws IOException {
        builder.field("total", totalWrites);
        builder.field("succeeded", succeededWrites);
        builder.field("failed", failedWrites);
        return builder;
    }

    public static WriterResult readNew(StreamInput in) throws IOException {
        WriterResult r = new WriterResult();
        r.readFrom(in);
        return r;
    }
}
