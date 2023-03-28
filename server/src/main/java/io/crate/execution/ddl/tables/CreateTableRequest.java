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

import io.crate.metadata.RelationName;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import io.crate.common.unit.TimeValue;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.support.master.AcknowledgedRequest.DEFAULT_ACK_TIMEOUT;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

/**
 * Creates a table represented by an ES index or an ES template (partitioned table).
 * Checks for existing views in the meta data of the master node before creating the table.
 *
 */
public class CreateTableRequest extends MasterNodeRequest<CreateTableRequest> implements AckedRequest {

    // Create table fields is superset of adding column(s).
    // Additional primary keys that are not inline with a column definition already merged with column PK-s before creating the request.
    // Check constraints have the same format (name -> expression) regardless of table/column level
    // and from the "mapping._meta" point of view is the same thing.
    private final AddColumnRequest addColumnRequest;

    // Below are fields specific only to CREATE TABLE.
    // Aligned with BoundCreateTable.mapping(), everything what's not covered by AddColumnRequest is added as a separate field.
    private final Settings settings;
    private final String routingColumn;
    private final String tableColumnPolicy; // The only setting which is set as a "mapping" change (see TableParameter.mappings())
    private final List<List<String>> partitionedBy;
    private final Map<String, Object> indicesMap;


    // Pre 5.3 fields
    private final CreateIndexRequest createIndexRequest;
    private final PutIndexTemplateRequest putIndexTemplateRequest;

    public CreateTableRequest(AddColumnRequest addColumnRequest,
                              Settings settings,
                              String routingColumn,
                              String tableColumnPolicy,
                              List<List<String>> partitionedBy,
                              Map<String, Object> indicesMap) {
        this.addColumnRequest = addColumnRequest;
        this.settings = settings;
        this.routingColumn = routingColumn;
        this.tableColumnPolicy = tableColumnPolicy;
        this.partitionedBy = partitionedBy;
        this.indicesMap = indicesMap;

        this.createIndexRequest = null;
        this.putIndexTemplateRequest = null;
    }

    public CreateTableRequest(CreateIndexRequest createIndexRequest) {
        this.createIndexRequest = createIndexRequest;
        this.putIndexTemplateRequest = null;

        this.addColumnRequest = null;
        this.settings = null;
        this.routingColumn = null;
        this.tableColumnPolicy = null;
        this.partitionedBy = null;
        this.indicesMap = null;
    }

    public CreateTableRequest(PutIndexTemplateRequest putIndexTemplateRequest) {
        this.createIndexRequest = null;
        this.putIndexTemplateRequest = putIndexTemplateRequest;

        this.addColumnRequest = null;
        this.settings = null;
        this.routingColumn = null;
        this.tableColumnPolicy = null;
        this.partitionedBy = null;
        this.indicesMap = null;
    }

    @Nullable
    public CreateIndexRequest getCreateIndexRequest() {
        return createIndexRequest;
    }

    @Nullable
    public PutIndexTemplateRequest getPutIndexTemplateRequest() {
        return putIndexTemplateRequest;
    }

    public RelationName getTableName() {
        if (createIndexRequest != null) {
            return RelationName.fromIndexName(createIndexRequest.index());
        } else if (putIndexTemplateRequest != null) {
            return RelationName.fromIndexName(putIndexTemplateRequest.aliases().iterator().next().name());
        } else if (addColumnRequest != null) {
            return addColumnRequest.relationName();
        } else {
            throw new IllegalStateException("Unknown request type");
        }
    }

    @Override
    public TimeValue ackTimeout() {
        return DEFAULT_ACK_TIMEOUT;
    }

    public CreateTableRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_5_3_0)) {
            this.addColumnRequest = new AddColumnRequest(in);
            this.settings = readSettingsFromStream(in);
            this.routingColumn = in.readString();
            this.tableColumnPolicy = in.readString();
            this.partitionedBy = in.readList(StreamInput::readStringList);
            this.indicesMap = in.readMap();

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
            this.addColumnRequest = null;
            this.settings = null;
            this.routingColumn = null;
            this.tableColumnPolicy = null;
            this.partitionedBy = null;
            this.indicesMap = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_5_3_0)) {
            addColumnRequest.writeTo(out);
            writeSettingsToStream(settings, out);
            out.writeString(routingColumn);
            out.writeString(tableColumnPolicy);
            out.writeCollection(partitionedBy, StreamOutput::writeStringCollection);
            out.writeMap(indicesMap);
        } else {
            boolean isIndexRequest = createIndexRequest != null;
            out.writeBoolean(isIndexRequest);
            MasterNodeRequest request = isIndexRequest ? createIndexRequest : putIndexTemplateRequest;
            request.writeTo(out);
        }
    }

    public AddColumnRequest addColumnRequest() {
        return addColumnRequest;
    }

    public Settings settings() {
        return settings;
    }

    public String routingColumn() {
        return routingColumn;
    }

    public String tableColumnPolicy() {
        return tableColumnPolicy;
    }

    public List<List<String>> partitionedBy() {
        return partitionedBy;
    }

    public Map<String, Object> indicesMap() {
        return indicesMap;
    }

}
