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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

/**
 * Request class for regular/single SQL statements.
 */
public class SQLRequest extends SQLBaseRequest {

    public final static Object[] EMPTY_ARGS = new Object[0];
    private Object[] args;

    public SQLRequest() {} // used for serialization

    public SQLRequest(String stmt) {
        super(stmt);
        args = EMPTY_ARGS;
    }

    public SQLRequest(String stmt, Object[] args) {
        super(stmt);
        args(args);
    }

    /**
     * @return the arguments of the request that have been set using
     * {@link #SQLRequest(String, Object[])} or {@link #args(Object[])}
     */
    public Object[] args() {
        return args;
    }

    /**
     * use to set the request arguments.
     *
     * E.g. if a statement like
     *
     *      "select * from x where y = ?"
     *
     *  is used, the value for "?" can be set as an argument.
     *  args in that case would look like:
     *
     *      args = new Object[] { "myYvalue" }
     */
    public void args(Object[] args) {
        this.args = MoreObjects.firstNonNull(args, EMPTY_ARGS);
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
        out.writeVLong(creationTime);
        out.writeBoolean(includeTypesOnResponse);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("stmt", stmt)
                .add("args", Arrays.asList(args))
                .add("creationTime", creationTime).toString();
    }
}
