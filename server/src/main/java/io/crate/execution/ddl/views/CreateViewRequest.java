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

package io.crate.execution.ddl.views;

import io.crate.metadata.RelationName;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import io.crate.common.unit.TimeValue;

import javax.annotation.Nullable;
import java.io.IOException;

import static org.elasticsearch.action.support.master.AcknowledgedRequest.DEFAULT_ACK_TIMEOUT;

public final class CreateViewRequest extends MasterNodeRequest<CreateViewRequest> implements AckedRequest {

    private final RelationName name;
    private final String query;
    private final boolean replaceExisting;
    @Nullable
    private final String owner;

    public CreateViewRequest(RelationName name, String query, boolean replaceExisting, @Nullable String owner) {
        this.name = name;
        this.query = query;
        this.replaceExisting = replaceExisting;
        this.owner = owner;
    }

    public RelationName name() {
        return name;
    }

    public String query() {
        return query;
    }

    boolean replaceExisting() {
        return replaceExisting;
    }

    @Nullable
    String owner() {
        return owner;
    }

    @Override
    public TimeValue ackTimeout() {
        return DEFAULT_ACK_TIMEOUT;
    }

    public CreateViewRequest(StreamInput in) throws IOException {
        super(in);
        name = new RelationName(in);
        query = in.readString();
        replaceExisting = in.readBoolean();
        owner = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        name.writeTo(out);
        out.writeString(query);
        out.writeBoolean(replaceExisting);
        out.writeOptionalString(owner);
    }
}
