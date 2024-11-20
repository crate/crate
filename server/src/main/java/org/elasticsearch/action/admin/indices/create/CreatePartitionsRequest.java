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

package org.elasticsearch.action.admin.indices.create;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.common.collections.Lists;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public class CreatePartitionsRequest extends AcknowledgedRequest<CreatePartitionsRequest> {

    private final RelationName relationName;
    private final List<List<String>> partitionValuesList;

    /**
     * @param partitions partitions to create. Limited to one unique relation
     **/
    public static CreatePartitionsRequest of(Collection<PartitionName> partitions) {
        List<RelationName> relations = partitions.stream()
            .map(PartitionName::relationName)
            .distinct()
            .toList();
        return switch (relations.size()) {
            case 0 -> throw new IllegalArgumentException("Must create at least one partition");
            case 1 -> new CreatePartitionsRequest(relations.get(0), Lists.map(partitions, PartitionName::values));
            default -> throw new IllegalArgumentException("Cannot create partitions for more than one table in the same request");
        };
    }

    public CreatePartitionsRequest(RelationName relationName, List<List<String>> partitionValuesList) {
        this.relationName = relationName;
        this.partitionValuesList = partitionValuesList;
    }

    public RelationName relationName() {
        return relationName;
    }

    public List<List<String>> partitionValuesList() {
        return partitionValuesList;
    }

    /**
     * @return index names created from {@link #relationName()} and {@link #partitionValuesList()}
     **/
    public List<String> indexNames() {
        return Lists.map(partitionValuesList, values -> new PartitionName(relationName, values).asIndexName());
    }

    public CreatePartitionsRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_5_10_0)) {
            this.relationName = new RelationName(in);
            int numPartitions = in.readVInt();
            this.partitionValuesList = new ArrayList<>(numPartitions);
            for (int i = 0; i < numPartitions; i++) {
                int numValues = in.readVInt();
                List<String> partitionValues = new ArrayList<>(numValues);
                for (int j = 0; j < numValues; j++) {
                    partitionValues.add(in.readOptionalString());
                }
                this.partitionValuesList.add(partitionValues);
            }
        } else {
            if (in.getVersion().before(Version.V_5_3_0)) {
                // The only usage of jobId was removed in https://github.com/crate/crate/commit/31e0f7f447eaa006e756c20bd32346b2680ebee6
                // Nodes < 5.3.0 still send UUID which is written as 2 longs, we consume them but don't create an UUID out of them.
                in.readLong();
                in.readLong();
            }
            int numIndices = in.readVInt();
            this.partitionValuesList = new ArrayList<>(numIndices);
            RelationName relation = null;
            for (int i = 0; i < numIndices; i++) {
                PartitionName partitionName = PartitionName.fromIndexOrTemplate(in.readString());
                if (relation == null) {
                    relation = partitionName.relationName();
                }
                partitionValuesList.add(partitionName.values());
            }
            this.relationName = relation;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_5_10_0)) {
            relationName.writeTo(out);
            out.writeVInt(partitionValuesList.size());
            for (List<String> partitionValues : partitionValuesList) {
                out.writeVInt(partitionValues.size());
                for (String value : partitionValues) {
                    out.writeOptionalString(value);
                }
            }
        } else {
            if (out.getVersion().before(Version.V_5_3_0)) {
                // Nodes < 5.3.0 still expect 2 longs.
                // They are used to construct an UUID but last time they were actually used in CrateDB 0.55.0.
                // Hence, sending dummy values.
                out.writeLong(0L);
                out.writeLong(0L);
            }

            out.writeVInt(partitionValuesList.size());
            for (List<String> partitionValues : partitionValuesList) {
                String indexName = new PartitionName(relationName, partitionValues).asIndexName();
                out.writeString(indexName);
            }
        }
    }
}
