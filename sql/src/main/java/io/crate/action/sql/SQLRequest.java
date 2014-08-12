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

package io.crate.action.sql;

import com.google.common.base.Objects;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

public class SQLRequest extends ActionRequest<SQLRequest> {

    private final static Object[] EMPTY_ARGS = new Object[0];
    private final static Object[][] EMPTY_BULK_ARGS = new Object[0][];

    private String stmt;
    private Object[] args;
    private Object[][] bulkArgs;
    private long creationTime;
    private boolean includeTypesOnResponse = false;

    public SQLRequest(String stmt, Object[][] bulkArgs) {
        this.stmt = stmt;
        this.args = EMPTY_ARGS;
        this.bulkArgs = Objects.firstNonNull(bulkArgs, EMPTY_BULK_ARGS);
    }

    public SQLRequest(String stmt, Object[] args) {
        this.stmt = stmt;
        args(args);
        this.bulkArgs = EMPTY_BULK_ARGS;
        this.creationTime = System.currentTimeMillis();
    }

    public SQLRequest(String stmt) {
        this(stmt, EMPTY_ARGS);
    }

    public SQLRequest() {
        this(null, EMPTY_ARGS);
    }

    public String stmt() {
        return stmt;
    }

    public Object[] args() {
        return args;
    }

    public Object[][] bulkArgs() {
        return bulkArgs;
    }

    public void args(Object[] args) {
        this.args = Objects.firstNonNull(args, EMPTY_ARGS);
    }

    public void bulkArgs(Object[][] bulkArgs){
        this.bulkArgs = Objects.firstNonNull(bulkArgs, EMPTY_BULK_ARGS);
    }

    public SQLRequest stmt(String stmt){
        this.stmt = stmt;
        return this;
    }

    public void includeTypesOnResponse(boolean includeTypesOnResponse) {
        this.includeTypesOnResponse = includeTypesOnResponse;
    }

    public boolean includeTypesOnResponse() {
        return includeTypesOnResponse;
    }

    public long creationTime() {
        return creationTime;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (stmt == null) {
            ActionRequestValidationException e =  new ActionRequestValidationException();
            e.addValidationError("Attribute 'stmt' must not be null");
            return e;
        }
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        stmt = in.readString();
        int length = in.readVInt();
        args = new Object[length];
        for (int i = 0; i < length; i++) {
            args[i] = in.readGenericValue();
        }
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
        creationTime = in.readVLong();
        includeTypesOnResponse = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(stmt);
        out.writeVInt(args.length);
        for (int i = 0; i < args.length; i++) {
            out.writeGenericValue(args[i]);
        }
        out.writeVInt(bulkArgs.length);
        for (int i = 0, bulkArgsLength = bulkArgs.length; i < bulkArgsLength; i++) {
            Object[] bulkArg = bulkArgs[i];
            out.writeVInt(bulkArg.length);
            for (int i1 = 0, bulkArgLength = bulkArg.length; i1 < bulkArgLength; i1++) {
                Object o = bulkArg[i1];
                out.writeGenericValue(o);
            }
        }
        out.writeVLong(creationTime);
        out.writeBoolean(includeTypesOnResponse);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("stmt", stmt)
                .add("args", Arrays.asList(args))
                .add("creationTime", creationTime).toString();
    }
}
