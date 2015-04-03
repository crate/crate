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

import com.google.common.base.Objects;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

public class SQLBulkRequest extends SQLBaseRequest {

    public final static Object[][] EMPTY_BULK_ARGS = new Object[0][];
    private Object[][] bulkArgs;

    public SQLBulkRequest() { // used for serialization

    }

    public SQLBulkRequest(String stmt) {
        this(stmt, EMPTY_BULK_ARGS);
    }

    public SQLBulkRequest(String stmt, Object[][] bulkArgs) {
        super(stmt);
        bulkArgs(bulkArgs);
    }

    public Object[][] bulkArgs() {
        return bulkArgs;
    }

    public void bulkArgs(Object[][] bulkArgs){
        this.bulkArgs = Objects.firstNonNull(bulkArgs, EMPTY_BULK_ARGS);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        stmt = in.readString();
        creationTime = in.readVLong();
        includeTypesOnResponse = in.readBoolean();

        int bulkArgsLength = in.readVInt();
        if (bulkArgsLength == 0) {
            bulkArgs = EMPTY_BULK_ARGS;
        } else {
            bulkArgs = new Object[bulkArgsLength][];
            for (int i = 0; i < bulkArgsLength; i++) {
                int bulkArgLength = in.readVInt();
                bulkArgs[i] = new Object[bulkArgLength];
                for (int j = 0; j < bulkArgLength; j++) {
                    bulkArgs[i][j] = in.readGenericValue();
                }
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeString(stmt);
        out.writeVLong(creationTime);
        out.writeBoolean(includeTypesOnResponse);

        out.writeVInt(bulkArgs.length);
        for (int i = 0, bulkArgsLength = bulkArgs.length; i < bulkArgsLength; i++) {
            Object[] bulkArg = bulkArgs[i];
            out.writeVInt(bulkArg.length);
            for (int i1 = 0, bulkArgLength = bulkArg.length; i1 < bulkArgLength; i1++) {
                out.writeGenericValue(bulkArg[i1]);
            }
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("stmt", stmt)
                .add("bulkArgs", Arrays.asList(bulkArgs))
                .add("creationTime", creationTime).toString();
    }
}
