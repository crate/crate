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

import static org.elasticsearch.action.support.master.AcknowledgedRequest.DEFAULT_ACK_TIMEOUT;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataTypes;

/**
 * Creates a table represented by an ES index or an ES template (partitioned table).
 * Checks for existing views in the meta data of the master node before creating the table.
 */
public class CreateTableRequest extends MasterNodeRequest<CreateTableRequest> implements AckedRequest {

    // Fields required to add column(s), aligned with AddColumnRequest
    private final RelationName relationName;
    private final List<Reference> columns;
    @Nullable
    private final String pkConstraintName;
    private final IntArrayList pKeyIndices;
    private final Map<String, String> checkConstraints;

    // Everything what's not covered by AddColumnRequest is added as a separate field.
    private final Settings settings;
    @Nullable
    private final ColumnIdent routingColumn;
    private final ColumnPolicy tableColumnPolicy; // The only setting which is set as a "mapping" change (see TableParameter.mappings()), 'strict' by default.
    private final List<ColumnIdent> partitionedBy;

    public CreateTableRequest(RelationName relationName,
                              @Nullable String pkConstraintName,
                              List<Reference> columns,
                              IntArrayList pKeyIndices,
                              Map<String, String> checkConstraints,
                              Settings settings,
                              @Nullable ColumnIdent routingColumn,
                              ColumnPolicy tableColumnPolicy,
                              List<ColumnIdent> partitionedBy) {
        this.relationName = relationName;
        this.pkConstraintName = pkConstraintName;
        this.columns = columns;
        this.pKeyIndices = pKeyIndices;
        this.checkConstraints = checkConstraints;
        this.settings = settings;
        this.routingColumn = routingColumn;
        this.tableColumnPolicy = tableColumnPolicy;
        this.partitionedBy = partitionedBy;
    }

    @NotNull
    public RelationName getTableName() {
        return relationName;
    }

    @Nullable
    public String pkConstraintName() {
        return pkConstraintName;
    }

    @Override
    public TimeValue ackTimeout() {
        return DEFAULT_ACK_TIMEOUT;
    }

    public CreateTableRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_5_6_0)) {
            this.pkConstraintName = in.readOptionalString();
        } else {
            this.pkConstraintName = null;
        }
        this.relationName = new RelationName(in);
        this.checkConstraints = in.readMap(
            LinkedHashMap::new, StreamInput::readString, StreamInput::readString);
        this.columns = in.readList(Reference::fromStream);
        int numPKIndices = in.readVInt();
        this.pKeyIndices = new IntArrayList(numPKIndices);
        for (int i = 0; i < numPKIndices; i++) {
            pKeyIndices.add(in.readVInt());
        }

        this.settings = readSettingsFromStream(in);
        boolean after510 = in.getVersion().onOrAfter(Version.V_5_10_0);
        if (after510) {
            this.routingColumn = in.readOptionalWriteable(ColumnIdent::of);
        } else {
            this.routingColumn = ColumnIdent.fromPath(in.readOptionalString());
        }
        this.tableColumnPolicy = ColumnPolicy.VALUES.get(in.readVInt());
        if (after510) {
            this.partitionedBy = in.readList(ColumnIdent::of);
        } else {
            // List of [fqn, typeMappingName]
            List<List<String>> partitionedBy = in.readList(StreamInput::readStringList);
            this.partitionedBy = new ArrayList<>(partitionedBy.size());
            for (List<String> column : partitionedBy) {
                this.partitionedBy.add(ColumnIdent.fromPath(column.get(0)));
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_5_6_0)) {
            out.writeOptionalString(pkConstraintName);
        }
        relationName.writeTo(out);
        out.writeMap(checkConstraints, StreamOutput::writeString, StreamOutput::writeString);
        out.writeCollection(columns, Reference::toStream);
        out.writeVInt(pKeyIndices.size());
        for (int i = 0; i < pKeyIndices.size(); i++) {
            out.writeVInt(pKeyIndices.get(i));
        }
        writeSettingsToStream(out, settings);
        boolean after510 = out.getVersion().onOrAfter(Version.V_5_10_0);
        if (after510) {
            out.writeOptionalWriteable(routingColumn);
        } else {
            out.writeOptionalString(routingColumn == null ? null : routingColumn.fqn());
        }
        out.writeVInt(tableColumnPolicy.ordinal());
        if (after510) {
            out.writeList(partitionedBy);
        } else {
            out.writeVInt(partitionedBy.size());
            for (ColumnIdent column : partitionedBy) {
                int refIdx = Reference.indexOf(columns, column);
                Reference reference = columns.get(refIdx);
                out.writeVInt(2);
                out.writeString(column.fqn());
                out.writeString(DataTypes.esMappingNameFrom(reference.valueType().id()));
            }
        }
    }

    @NotNull
    public Settings settings() {
        return settings;
    }

    @Nullable
    public ColumnIdent routingColumn() {
        return routingColumn;
    }

    @NotNull
    public ColumnPolicy tableColumnPolicy() {
        return tableColumnPolicy;
    }

    @NotNull
    public List<ColumnIdent> partitionedBy() {
        return partitionedBy;
    }

    @NotNull
    public Map<String, String> checkConstraints() {
        return this.checkConstraints;
    }

    @NotNull
    public List<Reference> references() {
        return this.columns;
    }

    @NotNull
    public IntArrayList pKeyIndices() {
        return this.pKeyIndices;
    }
}
