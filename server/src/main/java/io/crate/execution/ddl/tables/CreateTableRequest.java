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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.ColumnPolicy;

/**
 * Creates a table represented by an ES index or an ES template (partitioned table).
 * Checks for existing views in the meta data of the master node before creating the table.
 */
public class CreateTableRequest extends MasterNodeRequest<CreateTableRequest> implements AckedRequest {

    // Fields required to add column(s), aligned with AddColumnRequest
    private final RelationName relationName;
    private final List<Reference> colsToAdd;
    @Nullable
    private final String pkConstraintName;
    private final IntArrayList pKeyIndices;
    private final Map<String, String> checkConstraints;

    // Everything what's not covered by AddColumnRequest is added as a separate field.
    private final Settings settings;
    @Nullable
    private final String routingColumn;
    private final ColumnPolicy tableColumnPolicy; // The only setting which is set as a "mapping" change (see TableParameter.mappings()), 'strict' by default.
    private final List<List<String>> partitionedBy;

    @Deprecated
    private CreateIndexRequest createIndexRequest;
    @Deprecated
    private PutIndexTemplateRequest putIndexTemplateRequest;

    public CreateTableRequest(RelationName relationName,
                              @Nullable String pkConstraintName,
                              List<Reference> colsToAdd,
                              IntArrayList pKeyIndices,
                              Map<String, String> checkConstraints,
                              Settings settings,
                              @Nullable String routingColumn,
                              ColumnPolicy tableColumnPolicy,
                              List<List<String>> partitionedBy) {
        this.relationName = relationName;
        this.pkConstraintName = pkConstraintName;
        this.colsToAdd = colsToAdd;
        this.pKeyIndices = pKeyIndices;
        this.checkConstraints = checkConstraints;
        this.settings = settings;
        this.routingColumn = routingColumn;
        this.tableColumnPolicy = tableColumnPolicy;
        this.partitionedBy = partitionedBy;

        this.createIndexRequest = null;
        this.putIndexTemplateRequest = null;
    }

    @Deprecated
    public CreateTableRequest(CreateIndexRequest createIndexRequest) {
        this(RelationName.fromIndexName(createIndexRequest.index()),
            null,
            List.of(),
            new IntArrayList(),
            Map.of(),
            Settings.EMPTY,
            null,
            ColumnPolicy.STRICT,
            List.of()
        );
        this.createIndexRequest = createIndexRequest;
        this.putIndexTemplateRequest = null;
    }

    @Deprecated
    public CreateTableRequest(PutIndexTemplateRequest putIndexTemplateRequest) {
        this(RelationName.fromIndexName(putIndexTemplateRequest.aliases().iterator().next().name()),
            null,
            List.of(),
            new IntArrayList(),
            Map.of(),
            Settings.EMPTY,
            null,
            ColumnPolicy.STRICT,
            List.of()
        );
        this.createIndexRequest = null;
        this.putIndexTemplateRequest = putIndexTemplateRequest;
    }

    @Nullable
    public CreateIndexRequest getCreateIndexRequest() {
        return createIndexRequest;
    }

    @Nullable
    public PutIndexTemplateRequest getPutIndexTemplateRequest() {
        return putIndexTemplateRequest;
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
        if (in.getVersion().onOrAfter(Version.V_5_4_0)) {
            this.relationName = new RelationName(in);
            this.checkConstraints = in.readMap(
                LinkedHashMap::new, StreamInput::readString, StreamInput::readString);
            this.colsToAdd = in.readList(Reference::fromStream);
            int numPKIndices = in.readVInt();
            this.pKeyIndices = new IntArrayList(numPKIndices);
            for (int i = 0; i < numPKIndices; i++) {
                pKeyIndices.add(in.readVInt());
            }

            this.settings = readSettingsFromStream(in);
            this.routingColumn = in.readOptionalString();
            this.tableColumnPolicy = ColumnPolicy.VALUES.get(in.readVInt());
            this.partitionedBy = in.readList(StreamInput::readStringList);

            createIndexRequest = null;
            putIndexTemplateRequest = null;
        } else {
            if (in.readBoolean()) {
                createIndexRequest = new CreateIndexRequest(in);
                putIndexTemplateRequest = null;
            } else {
                putIndexTemplateRequest = new PutIndexTemplateRequest(in);
                createIndexRequest = null;
            }
            this.relationName = null;
            this.colsToAdd = null;
            this.pKeyIndices = null;
            this.checkConstraints = null;
            this.settings = null;
            this.routingColumn = null;
            this.tableColumnPolicy = null;
            this.partitionedBy = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_5_6_0)) {
            out.writeOptionalString(pkConstraintName);
        }
        if (out.getVersion().onOrAfter(Version.V_5_4_0)) {
            relationName.writeTo(out);
            out.writeMap(checkConstraints, StreamOutput::writeString, StreamOutput::writeString);
            out.writeCollection(colsToAdd, Reference::toStream);
            out.writeVInt(pKeyIndices.size());
            for (int i = 0; i < pKeyIndices.size(); i++) {
                out.writeVInt(pKeyIndices.get(i));
            }
            writeSettingsToStream(settings, out);
            out.writeOptionalString(routingColumn);
            out.writeVInt(tableColumnPolicy.ordinal());
            out.writeCollection(partitionedBy, StreamOutput::writeStringCollection);
        } else {
            boolean isIndexRequest = createIndexRequest != null;
            out.writeBoolean(isIndexRequest);
            var request = isIndexRequest ? createIndexRequest : putIndexTemplateRequest;
            request.writeTo(out);
        }
    }

    @NotNull
    public Settings settings() {
        return settings;
    }

    @Nullable
    public String routingColumn() {
        return routingColumn;
    }

    @NotNull
    public ColumnPolicy tableColumnPolicy() {
        return tableColumnPolicy;
    }

    @NotNull
    public List<List<String>> partitionedBy() {
        return partitionedBy;
    }

    @NotNull
    public Map<String, String> checkConstraints() {
        return this.checkConstraints;
    }

    @NotNull
    public List<Reference> references() {
        return this.colsToAdd;
    }

    @NotNull
    public IntArrayList pKeyIndices() {
        return this.pKeyIndices;
    }
}
