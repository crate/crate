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

import javax.annotation.Nullable;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.metadata.RelationName;

public final class CloseTableRequest extends AcknowledgedRequest<CloseTableRequest> {

    private final RelationName table;
    private final String partition;

    public CloseTableRequest(RelationName table, @Nullable String partition) {
        this.table = table;
        this.partition = partition;
    }

    public CloseTableRequest(StreamInput in) throws IOException {
        super(in);
        this.table = new RelationName(in);
        this.partition = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        table.writeTo(out);
        out.writeOptionalString(partition);
    }

    public RelationName table() {
        return table;
    }

    @Nullable
    public String partition() {
        return partition;
    }
}
