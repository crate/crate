/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.ddl.tables;

import io.crate.metadata.RelationName;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import javax.annotation.Nullable;
import java.io.IOException;

import static org.elasticsearch.action.support.master.AcknowledgedRequest.DEFAULT_ACK_TIMEOUT;

/**
 * Creates a table represented by an ES index or an ES template (partitioned table).
 * Checks for existing views in the meta data of the master node before creating the table.
 *
 * Encapsulates either a {@link CreateIndexRequest} or a {@link PutIndexTemplateRequest}.
 */
public class CreateTableRequest extends MasterNodeRequest<CreateTableRequest> implements AckedRequest {

    private CreateIndexRequest createIndexRequest;
    private PutIndexTemplateRequest putIndexTemplateRequest;

    public CreateTableRequest(CreateIndexRequest createIndexRequest) {
        this.createIndexRequest = createIndexRequest;
    }

    public CreateTableRequest(PutIndexTemplateRequest putIndexTemplateRequest) {
        this.putIndexTemplateRequest = putIndexTemplateRequest;
    }

    CreateTableRequest() {
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
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public TimeValue ackTimeout() {
        return DEFAULT_ACK_TIMEOUT;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            createIndexRequest = new CreateIndexRequest();
            createIndexRequest.readFrom(in);
        } else {
            putIndexTemplateRequest = new PutIndexTemplateRequest();
            putIndexTemplateRequest.readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        boolean isIndexRequest = createIndexRequest != null;
        out.writeBoolean(isIndexRequest);
        MasterNodeRequest request = isIndexRequest ? createIndexRequest : putIndexTemplateRequest;
        request.writeTo(out);
    }
}
