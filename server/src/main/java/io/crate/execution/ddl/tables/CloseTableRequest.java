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


package io.crate.execution.ddl.tables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.metadata.RelationName;

public class CloseTableRequest extends AcknowledgedRequest<CloseTableRequest> {

    private final List<RelationName> tables;
    private final String partition;

    public CloseTableRequest(List<RelationName> tables, @Nullable String partition) {
        this.tables = tables;
        this.partition = partition;
    }

    public CloseTableRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.fromString("4.8.0"))) {
            tables = List.of(new RelationName(in));
        } else {
            int count = in.readVInt();
            tables = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                tables.add(new RelationName(in));
            }
        }
        partition = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.fromString("4.8.0"))) {
            // Before 4.8.0 request was used only for closing table with non-null RelationName field.
            // In this branch it's guaranteed that we are not passing empty list
            tables.get(0).writeTo(out);
        } else {
            out.writeVInt(tables.size());
            for (int i = 0; i < tables.size(); i++) {
                tables.get(i).writeTo(out);
            }
        }
        out.writeOptionalString(partition);
    }

    public List<RelationName> tables() {
        return tables;
    }

    @Nullable
    public String partition() {
        return partition;
    }
}
