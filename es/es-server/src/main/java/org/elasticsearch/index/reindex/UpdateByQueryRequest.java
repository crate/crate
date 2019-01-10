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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

/**
 * Request to update some documents. That means you can't change their type, id, index, or anything like that. This implements
 * CompositeIndicesRequest but in a misleading way. Rather than returning all the subrequests that it will make it tries to return a
 * representative set of subrequests. This is best-effort but better than {@linkplain ReindexRequest} because scripts can't change the
 * destination index and things.
 */
public class UpdateByQueryRequest extends AbstractBulkIndexByScrollRequest<UpdateByQueryRequest>
    implements IndicesRequest.Replaceable, ToXContentObject {
    /**
     * Ingest pipeline to set on index requests made by this action.
     */
    private String pipeline;

    public UpdateByQueryRequest() {
        this(new SearchRequest());
    }

    public UpdateByQueryRequest(String... indices) {
        this(new SearchRequest(indices));
    }

    UpdateByQueryRequest(SearchRequest search) {
        this(search, true);
    }

    public UpdateByQueryRequest(StreamInput in) throws IOException {
        super.readFrom(in);
        pipeline = in.readOptionalString();
    }

    private UpdateByQueryRequest(SearchRequest search, boolean setDefaults) {
        super(search, setDefaults);
    }

    /**
     * Set the ingest pipeline to set on index requests made by this action.
     */
    public UpdateByQueryRequest setPipeline(String pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    /**
     * Set the query for selective update
     */
    public UpdateByQueryRequest setQuery(QueryBuilder query) {
        if (query != null) {
            getSearchRequest().source().query(query);
        }
        return this;
    }

    /**
     * Set the document types for the update
     */
    public UpdateByQueryRequest setDocTypes(String... types) {
        if (types != null) {
            getSearchRequest().types(types);
        }
        return this;
    }

    /**
     * Set routing limiting the process to the shards that match that routing value
     */
    public UpdateByQueryRequest setRouting(String routing) {
        if (routing != null) {
            getSearchRequest().routing(routing);
        }
        return this;
    }

    /**
     * The scroll size to control number of documents processed per batch
     */
    public UpdateByQueryRequest setBatchSize(int size) {
        getSearchRequest().source().size(size);
        return this;
    }

    /**
     * Set the IndicesOptions for controlling unavailable indices
     */
    public UpdateByQueryRequest setIndicesOptions(IndicesOptions indicesOptions) {
        getSearchRequest().indicesOptions(indicesOptions);
        return this;
    }

    /**
     * Gets the batch size for this request
     */
    public int getBatchSize() {
        return getSearchRequest().source().size();
    }

    /**
     * Gets the routing value used for this request
     */
    public String getRouting() {
        return getSearchRequest().routing();
    }

    /**
     * Gets the document types on which this request would be executed. Returns an empty array if all
     * types are to be processed.
     */
    public String[] getDocTypes() {
        if (getSearchRequest().types() != null) {
            return getSearchRequest().types();
        } else {
            return new String[0];
        }
    }

    /**
     * Ingest pipeline to set on index requests made by this action.
     */
    public String getPipeline() {
        return pipeline;
    }

    @Override
    protected UpdateByQueryRequest self() {
        return this;
    }

    @Override
    public UpdateByQueryRequest forSlice(TaskId slicingTask, SearchRequest slice, int totalSlices) {
        UpdateByQueryRequest request = doForSlice(new UpdateByQueryRequest(slice, false), slicingTask, totalSlices);
        request.setPipeline(pipeline);
        return request;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("update-by-query ");
        searchToString(b);
        return b.toString();
    }

    //update by query updates all documents that match a query. The indices and indices options that affect how
    //indices are resolved depend entirely on the inner search request. That's why the following methods delegate to it.
    @Override
    public IndicesRequest indices(String... indices) {
        assert getSearchRequest() != null;
        getSearchRequest().indices(indices);
        return this;
    }

    @Override
    public String[] indices() {
        assert getSearchRequest() != null;
        return getSearchRequest().indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        assert getSearchRequest() != null;
        return getSearchRequest().indicesOptions();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(pipeline);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (getScript() != null) {
            builder.field("script");
            getScript().toXContent(builder, params);
        }
        getSearchRequest().source().innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }
}
