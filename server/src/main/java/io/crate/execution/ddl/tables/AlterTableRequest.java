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

import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public class AlterTableRequest extends AcknowledgedRequest<AlterTableRequest> {

    private final RelationName table;
    private final List<String> partitionValues;
    private final boolean isPartitioned;
    private final boolean excludePartitions;
    private final Settings settings;

    public AlterTableRequest(RelationName table,
                             List<String> partitionValues,
                             boolean isPartitioned,
                             boolean excludePartitions,
                             Settings settings) throws IOException {
        this.table = table;
        this.partitionValues = partitionValues;
        this.isPartitioned = isPartitioned;
        this.excludePartitions = excludePartitions;
        this.settings = settings;
    }

    public AlterTableRequest(StreamInput in) throws IOException {
        super(in);
        table = new RelationName(in);
        boolean before510 = in.getVersion().before(Version.V_5_10_0);
        if (before510) {
            String partitionIndexName = in.readOptionalString();
            partitionValues = partitionIndexName == null
                ? List.of()
                : PartitionName.fromIndexOrTemplate(partitionIndexName).values();
        } else {
            int numValues = in.readVInt();
            partitionValues = new ArrayList<>(numValues);
            for (int i = 0; i < numValues; i++) {
                partitionValues.add(in.readOptionalString());
            }
        }
        isPartitioned = in.readBoolean();
        excludePartitions = in.readBoolean();
        settings = readSettingsFromStream(in);
        if (before510) {
            in.readOptionalString(); // mappingDelta
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        table.writeTo(out);
        boolean before510 = out.getVersion().before(Version.V_5_10_0);
        if (before510) {
            if (partitionValues.isEmpty()) {
                out.writeOptionalString(null);
            } else {
                String partitionIndexName = new PartitionName(table, partitionValues).asIndexName();
                out.writeOptionalString(partitionIndexName);
            }
        } else {
            out.writeVInt(partitionValues.size());
            for (String value : partitionValues) {
                out.writeOptionalString(value);
            }
        }
        out.writeBoolean(isPartitioned);
        out.writeBoolean(excludePartitions);
        writeSettingsToStream(out, settings);
        if (before510) {
            out.writeOptionalString(null);
        }
    }

    public RelationName tableIdent() {
        return table;
    }

    public List<String> partitionValues() {
        return partitionValues;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    public boolean excludePartitions() {
        return excludePartitions;
    }

    public Settings settings() {
        return settings;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + table.hashCode();
        result = prime * result + partitionValues.hashCode();
        result = prime * result + (isPartitioned ? 1231 : 1237);
        result = prime * result + (excludePartitions ? 1231 : 1237);
        result = prime * result + settings.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof AlterTableRequest other
            && table.equals(other.table)
            && partitionValues.equals(other.partitionValues)
            && isPartitioned == other.isPartitioned
            && excludePartitions == other.excludePartitions
            && settings.equals(other.settings);
    }
}
