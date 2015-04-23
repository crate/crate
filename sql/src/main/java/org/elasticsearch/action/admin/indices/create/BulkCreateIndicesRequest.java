/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class BulkCreateIndicesRequest extends AcknowledgedRequest<BulkCreateIndicesRequest> implements IndicesRequest {

    private List<CreateIndexRequest> createIndexRequests;

    private boolean ignoreExisting = false;

    /**
     * Constructs a new request to create indices with the specified names.
     */
    public BulkCreateIndicesRequest(Collection<String> indices) {
        this.createIndexRequests = new ArrayList<>(indices.size());
        for (String index : indices) {
            this.createIndexRequests.add(new CreateIndexRequest(index));
        }
    }

    /**
     * Constructs a new request to create indices in bulk.
     */
    BulkCreateIndicesRequest() {
    }

    public boolean ignoreExisting() {
        return ignoreExisting;
    }

    public BulkCreateIndicesRequest ignoreExisting(boolean ignoreExisting) {
        this.ignoreExisting = ignoreExisting;
        return this;
    }

    public BulkCreateIndicesRequest requests(List<CreateIndexRequest> requests) {
        this.createIndexRequests = requests;
        return this;
    }

    public Collection<CreateIndexRequest> requests() {
        return createIndexRequests;
    }

    public BulkCreateIndicesRequest add(CreateIndexRequest createIndexRequest) {
        if (createIndexRequests == null) {
            createIndexRequests = Lists.newArrayList();
        }
        createIndexRequests.add(createIndexRequest);
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        for (CreateIndexRequest createIndexRequest : createIndexRequests) {
            ActionRequestValidationException childValidationException = createIndexRequest.validate();
            if (childValidationException != null) {
                if (validationException == null) {
                    validationException = new ActionRequestValidationException();
                }
                validationException.addValidationErrors(childValidationException.validationErrors());
            }
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return Lists.transform(createIndexRequests, new Function<CreateIndexRequest, String>() {
            @Nullable
            @Override
            public String apply(CreateIndexRequest input) {
                return input.indices()[0];
            }
        }).toArray(new String[createIndexRequests.size()]);
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        ignoreExisting = in.readBoolean();
        int numRequests = in.readVInt();
        createIndexRequests = new ArrayList<>(numRequests);
        for (int i = 0; i < numRequests; i++) {
            CreateIndexRequest request = new CreateIndexRequest("");
            request.readFrom(in);
            createIndexRequests.add(request);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(ignoreExisting);

        if (createIndexRequests == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(createIndexRequests.size());
            for (CreateIndexRequest createIndexRequest : createIndexRequests) {
                createIndexRequest.writeTo(out);
            }
        }
    }
}
