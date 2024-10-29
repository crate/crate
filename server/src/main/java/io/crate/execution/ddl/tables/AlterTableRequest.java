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

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.RelationName;

public class AlterTableRequest extends AcknowledgedRequest<AlterTableRequest> {

    private final RelationName tableIdent;
    @Nullable
    private final String partitionIndexName;
    private final boolean isPartitioned;
    private final boolean excludePartitions;
    private final Settings settings;

    public AlterTableRequest(RelationName tableIdent,
                             @Nullable String partitionIndexName,
                             boolean isPartitioned,
                             boolean excludePartitions,
                             Settings settings) throws IOException {
        this.tableIdent = tableIdent;
        this.partitionIndexName = partitionIndexName;
        this.isPartitioned = isPartitioned;
        this.excludePartitions = excludePartitions;
        this.settings = settings;
    }

    public AlterTableRequest(StreamInput in) throws IOException {
        super(in);
        tableIdent = new RelationName(in);
        partitionIndexName = in.readOptionalString();
        isPartitioned = in.readBoolean();
        excludePartitions = in.readBoolean();
        settings = readSettingsFromStream(in);
        if (in.getVersion().before(Version.V_5_10_0)) {
            in.readOptionalString(); // mappingDelta
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        tableIdent.writeTo(out);
        out.writeOptionalString(partitionIndexName);
        out.writeBoolean(isPartitioned);
        out.writeBoolean(excludePartitions);
        writeSettingsToStream(out, settings);
        if (out.getVersion().before(Version.V_5_10_0)) {
            out.writeOptionalString(null);
        }
    }

    public RelationName tableIdent() {
        return tableIdent;
    }

    @Nullable
    public String partitionIndexName() {
        return partitionIndexName;
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
}
