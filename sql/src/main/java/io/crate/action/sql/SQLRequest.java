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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class SQLRequest extends ActionRequest<SQLRequest> {


    private String stmt;
    private Object[] args;
    private long creationTime;

    public SQLRequest(String stmt, Object[] args) {
        this.stmt = stmt;
        args(args);
        this.creationTime = System.currentTimeMillis();
    }

    public SQLRequest(String stmt) {
        this.stmt = stmt;
        this.args = new Object[0];
        this.creationTime = System.currentTimeMillis();
    }

    public SQLRequest() {
        this.creationTime = System.currentTimeMillis();
    }

    public String stmt() {
        return stmt;
    }

    public Object[] args() {
        return args;
    }

    public void args(Object[] args) {
        if (args == null) {
            this.args = new Object[0];
        } else {
            this.args = args;
        }
    }

    public SQLRequest stmt(String stmt){
        this.stmt = stmt;
        return this;
    }

    public long creationTime() {
        return creationTime;
    }

    @Override
    public ActionRequestValidationException validate() {
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
        creationTime = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(stmt);
        out.writeVInt(args.length);
        for (int i = 0; i < args.length; i++) {
            out.writeGenericValue(args[i]);
        }
        out.writeVLong(creationTime);
    }

}
