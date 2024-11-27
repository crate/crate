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

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public final class CloseTableRequest extends AcknowledgedRequest<CloseTableRequest> {

    private final RelationName table;
    private final List<String> partitionValues;

    public CloseTableRequest(RelationName table, List<String> partitionValues) {
        this.table = table;
        this.partitionValues = partitionValues;
    }

    public CloseTableRequest(StreamInput in) throws IOException {
        super(in);
        this.table = new RelationName(in);
        if (in.getVersion().onOrAfter(Version.V_5_10_0)) {
            int numValues = in.readVInt();
            this.partitionValues = new ArrayList<>(numValues);
            for (int i = 0; i < numValues; i++) {
                partitionValues.add(in.readOptionalString());
            }
        } else {
            String partition = in.readOptionalString();
            if (partition == null) {
                partitionValues = List.of();
            } else {
                partitionValues = PartitionName.fromIndexOrTemplate(partition).values();
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        table.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_5_10_0)) {
            out.writeVInt(partitionValues.size());
            for (String value : partitionValues) {
                out.writeOptionalString(value);
            }
        } else {
            if (partitionValues.isEmpty()) {
                out.writeBoolean(false);
            } else {
                PartitionName partitionName = new PartitionName(table, partitionValues);
                out.writeBoolean(true);
                out.writeString(partitionName.asIndexName());
            }
        }
    }

    public RelationName table() {
        return table;
    }

    public List<String> partitionValues() {
        return partitionValues;
    }
}
