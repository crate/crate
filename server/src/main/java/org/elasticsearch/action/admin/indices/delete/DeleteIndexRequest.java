/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.delete;

import java.io.IOException;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;


/**
 * A request to delete an index.
 */
public class DeleteIndexRequest extends AcknowledgedRequest<DeleteIndexRequest> implements IndicesRequest {

    private String[] indices;
    // Delete index should work by default on both open and closed indices.
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, true, true, true, false, false);

    /**
     * Constructs a new delete index request for the specified indices.
     *
     * @param indices The indices to delete. Use "_all" to delete all indices.
     */
    public DeleteIndexRequest(String... indices) {
        this.indices = indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public DeleteIndexRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * The index to delete.
     */
    @Override
    public String[] indices() {
        return indices;
    }

    public DeleteIndexRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
    }
}
