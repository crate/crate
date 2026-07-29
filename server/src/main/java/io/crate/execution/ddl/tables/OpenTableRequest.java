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

import static org.elasticsearch.cluster.metadata.Metadata.OID_UNASSIGNED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;


public class OpenTableRequest extends AcknowledgedRequest<OpenTableRequest> {

    private final RelationName relation;
    private final int tableOid;
    private final List<String> partitionValues;

    public OpenTableRequest(RelationName relation, int tableOid, List<String> partitionValues) {
        this.relation = relation;
        this.tableOid = tableOid;
        this.partitionValues = partitionValues;
    }

    public RelationName relation() {
        return relation;
    }

    public int tableOid() {
        return tableOid;
    }

    /**
     * Values from {@code ALTER TABLE tbl PARTITION (<pcol> = <value> [, ...]) OPEN}
     * <p>
     * Empty if PARTITION clause was not provided or table isn't partitioned.
     * </p>
     **/
    public List<String> partitionValues() {
        return partitionValues;
    }

    public OpenTableRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.V_5_10_0)) {
            relation = new RelationName(in);
            tableOid = OID_UNASSIGNED;
            String partitionIndexName = in.readOptionalString();
            if (partitionIndexName == null) {
                partitionValues = List.of();
            } else {
                partitionValues = PartitionName.fromIndexOrTemplate(partitionIndexName).values();
            }
            in.readBoolean(); // openTable flag; before 4.3.0 the request was also used to close tables
        } else {
            relation = new RelationName(in);
            if ((in.getVersion().before(Version.V_6_4_0) && in.getVersion().onOrAfter(Version.V_6_3_7))
                || in.getVersion().onOrAfter(Version.V_6_4_2)) {
                tableOid = in.readVInt();
            } else {
                tableOid = OID_UNASSIGNED;
            }
            int numValues = in.readVInt();
            partitionValues = new ArrayList<>(numValues);
            for (int i = 0; i < numValues; i++) {
                partitionValues.add(in.readOptionalString());
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_5_10_0)) {
            relation.writeTo(out);
            if (partitionValues.isEmpty()) {
                out.writeBoolean(false);
            } else {
                String indexName = new PartitionName(relation, partitionValues).asIndexName();
                out.writeBoolean(true);
                out.writeString(indexName);
            }
            out.writeBoolean(true); // openTable flag; before 4.3.0 the request was also used to close tables
        } else {
            relation.writeTo(out);
            if ((out.getVersion().before(Version.V_6_4_0) && out.getVersion().onOrAfter(Version.V_6_3_7))
                || out.getVersion().onOrAfter(Version.V_6_4_2)) {
                out.writeVInt(tableOid);
            }
            out.writeVInt(partitionValues.size());
            for (String partitionValue : partitionValues) {
                out.writeOptionalString(partitionValue);
            }
        }
    }
}
