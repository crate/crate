/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.action.sql;

import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.XContentBuilder;

import javax.annotation.Nullable;
import java.io.IOException;

public class SQLBulkResponse extends SQLBaseResponse {

    public static final Result[] EMPTY_RESULTS = new Result[0];

    private Result[] results;

    public SQLBulkResponse() {} // used for serialization

    public SQLBulkResponse(String[] outputNames,
                           Result[] results,
                           float duration,
                           DataType[] colTypes,
                           boolean includeTypes) {
        super(outputNames, colTypes, includeTypes, duration);
        this.results = results;
    }

    public Result[] results() {
        return results;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        writeSharedAttributes(builder);
        builder.startArray(Fields.RESULTS);
        for (Result result : results) {
            result.toXContent(builder);
        }
        builder.endArray();
        return builder;
    }


    public static class Result implements Streamable{

        private String errorMessage;
        private long rowCount;

        public Result() {} // used for serialization

        public Result(@Nullable String errorMessage, long rowCount) {
            this.errorMessage = errorMessage;
            this.rowCount = rowCount;
        }

        @Nullable
        public String errorMessage() {
            return errorMessage;
        }

        public long rowCount() {
            return rowCount;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            rowCount = in.readLong();
            errorMessage = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(rowCount);
            out.writeOptionalString(errorMessage);
        }

        public void toXContent(XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.field(Fields.ROWCOUNT, rowCount);
            if (errorMessage != null) {
                builder.field(Fields.ERROR_MESSAGE, errorMessage);
            }
            builder.endObject();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(results.length);
        for (Result result : results) {
            result.writeTo(out);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int numResults = in.readVInt();
        results = new Result[numResults];
        for (int i = 0; i < numResults; i++) {
            results[i] = new Result();
            results[i].readFrom(in);
        }
    }
}
