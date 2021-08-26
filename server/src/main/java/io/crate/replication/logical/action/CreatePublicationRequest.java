/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.replication.logical.action;

import io.crate.metadata.RelationName;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CreatePublicationRequest extends AcknowledgedRequest<CreatePublicationRequest> {

    private final String owner;
    private final String name;
    private final List<RelationName> tables;

    public CreatePublicationRequest(String owner, String name, List<RelationName> tables) {
        this.owner = owner;
        this.name = name;
        this.tables = tables;
    }

    public CreatePublicationRequest(StreamInput in) throws IOException {
        super(in);
        this.owner = in.readString();
        this.name = in.readString();
        int size = in.readVInt();
        var t = new ArrayList<RelationName>(size);
        for (var i = 0; i < size; i++) {
            t.add(new RelationName(in));
        }
        this.tables = List.copyOf(t);
    }

    public String owner() {
        return owner;
    }

    public String name() {
        return name;
    }

    public List<RelationName> tables() {
        return tables;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(owner);
        out.writeString(name);
        out.writeVInt(tables.size());
        for (var table : tables) {
            table.writeTo(out);
        }
    }
}
