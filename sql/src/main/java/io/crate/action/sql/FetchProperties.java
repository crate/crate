/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.action.sql;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class FetchProperties implements Streamable {

    // TODO: this default breaks backward compat for SQL requests :(
    public final static FetchProperties DEFAULT = new FetchProperties(100, false);

    private int fetchSize;
    private boolean closeContext;

    public FetchProperties(int fetchSize, boolean closeContext) {
        this.fetchSize = fetchSize;
        this.closeContext = closeContext;
    }

    public FetchProperties(StreamInput in) throws IOException {
        this(in.readVInt(), in.readBoolean());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        fetchSize = in.readVInt();
        closeContext = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(fetchSize);
        out.writeBoolean(closeContext);
    }

    public int fetchSize() {
        return fetchSize;
    }

    public boolean closeContext() {
        return closeContext;
    }
}
