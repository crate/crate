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

import static org.elasticsearch.action.support.master.AcknowledgedRequest.DEFAULT_ACK_TIMEOUT;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

/**
 * Creates a table represented by an ES index or an ES template (partitioned table).
 * Checks for existing views in the meta data of the master node before creating the table.
 *
 * Encapsulates either a {@link CreateIndexRequest} or a {@link PutIndexTemplateRequest}.
 */
public class CreateTableRequest extends MasterNodeRequest<CreateTableRequest> implements AckedRequest {

    private final RelationName relationName; // Used for computing templateName, pattern and indexNameOrAlias.
    private final Settings settings;
    private final String mapping;
    private final boolean isPartitioned;

    private final CreateIndexRequest createIndexRequest;
    private final PutIndexTemplateRequest putIndexTemplateRequest;

    public CreateTableRequest(RelationName relationName, Settings settings, String mapping, boolean isPartitioned) {
        this.relationName = relationName;
        this.settings = settings;
        this.mapping = mapping;
        this.isPartitioned = isPartitioned;
        this.createIndexRequest = null;
        this.putIndexTemplateRequest = null;

    }

    public CreateTableRequest(CreateIndexRequest createIndexRequest) {
        this.createIndexRequest = createIndexRequest;
        this.putIndexTemplateRequest = null;
        this.relationName = null;
        this.settings = null;
        this.mapping = null;
        this.isPartitioned = false;
    }

    public CreateTableRequest(PutIndexTemplateRequest putIndexTemplateRequest) {
        this.createIndexRequest = null;
        this.putIndexTemplateRequest = putIndexTemplateRequest;
        this.relationName = null;
        this.settings = null;
        this.mapping = null;
        this.isPartitioned = true; // Unused, but value corresponds to constructor semantics to avoid confusion.
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
            this.relationName = new RelationName(in);
            this.settings = readSettingsFromStream(in);
            this.mapping = in.readString();
            this.isPartitioned = in.readBoolean();
            createIndexRequest = null;
            putIndexTemplateRequest = null;
        } else {
            if (in.readBoolean()) {
                createIndexRequest = new CreateIndexRequest(in);
                putIndexTemplateRequest = null;
                this.isPartitioned = false;
            } else {
                putIndexTemplateRequest = new PutIndexTemplateRequest(in);
                createIndexRequest = null;
                this.isPartitioned = true; // Unused, but value corresponds to constructor semantics to avoid confusion.
            }
            this.relationName = null;
            this.settings = null;
            this.mapping = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_5_3_0)) {
            relationName.writeTo(out);
            writeSettingsToStream(settings, out);
            out.writeString(mapping);
            out.writeBoolean(isPartitioned);
        } else {
            boolean isIndexRequest = createIndexRequest != null;
            out.writeBoolean(isIndexRequest);
            MasterNodeRequest request = isIndexRequest ? createIndexRequest : putIndexTemplateRequest;
            request.writeTo(out);
        }
    }

    public RelationName relationName() {
        return relationName;
    }

    public Settings settings() {
        return settings;
    }

    public String mapping() {
        return mapping;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }
}
