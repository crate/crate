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

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public class DropPartitionsRequest extends AcknowledgedRequest<DropPartitionsRequest> {

    private final RelationName relationName;
    private final List<PartitionName> partitions;

    public DropPartitionsRequest(RelationName relationName, List<PartitionName> partitions) {
        this.relationName = relationName;
        this.partitions = partitions;
    }

    public RelationName relationName() {
        return relationName;
    }

    public List<PartitionName> partitions() {
        return partitions;
    }

    public DropPartitionsRequest(StreamInput in) throws IOException {
        super(in);
        relationName = new RelationName(in);
        int size = in.readVInt();
        partitions = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            int numValues = in.readVInt();
            ArrayList<String> values = new ArrayList<>(numValues);
            for (int j = 0; j < numValues; j++) {
                values.add(in.readOptionalString());
            }
            partitions.add(new PartitionName(relationName, values));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        relationName.writeTo(out);
        out.writeVInt(partitions.size());
        for (PartitionName partition : partitions) {
            assert partition.relationName().equals(relationName)
                : "RelationName of PartitionName must match requets' relationName";
            List<String> values = partition.values();
            out.writeVInt(values.size());
            for (String value : values) {
                out.writeOptionalString(value);
            }
        }
    }
}
