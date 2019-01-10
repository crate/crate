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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.index.VersionType.INTERNAL;

/**
 * Request to reindex some documents from one index to another. This implements CompositeIndicesRequest but in a misleading way. Rather than
 * returning all the subrequests that it will make it tries to return a representative set of subrequests. This is best-effort for a bunch
 * of reasons, not least of which that scripts are allowed to change the destination request in drastic ways, including changing the index
 * to which documents are written.
 */
public class ReindexRequest extends AbstractBulkIndexByScrollRequest<ReindexRequest>
                            implements CompositeIndicesRequest, ToXContentObject {
    /**
     * Prototype for index requests.
     */
    private IndexRequest destination;

    private RemoteInfo remoteInfo;

    public ReindexRequest() {
        this(new SearchRequest(), new IndexRequest(), true);
    }

    ReindexRequest(SearchRequest search, IndexRequest destination) {
        this(search, destination, true);
    }

    private ReindexRequest(SearchRequest search, IndexRequest destination, boolean setDefaults) {
        super(search, setDefaults);
        this.destination = destination;
    }

    public ReindexRequest(StreamInput in) throws IOException {
        super.readFrom(in);
        destination = new IndexRequest();
        destination.readFrom(in);
        remoteInfo = in.readOptionalWriteable(RemoteInfo::new);
    }

    @Override
    protected ReindexRequest self() {
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = super.validate();
        if (getSearchRequest().indices() == null || getSearchRequest().indices().length == 0) {
            e = addValidationError("use _all if you really want to copy from all existing indexes", e);
        }
        if (getSearchRequest().source().fetchSource() != null && getSearchRequest().source().fetchSource().fetchSource() == false) {
            e = addValidationError("_source:false is not supported in this context", e);
        }
        /*
         * Note that we don't call index's validator - it won't work because
         * we'll be filling in portions of it as we receive the docs. But we can
         * validate some things so we do that below.
         */
        if (destination.index() == null) {
            e = addValidationError("index must be specified", e);
            return e;
        }
        if (false == routingIsValid()) {
            e = addValidationError("routing must be unset, [keep], [discard] or [=<some new value>]", e);
        }
        if (destination.versionType() == INTERNAL) {
            if (destination.version() != Versions.MATCH_ANY && destination.version() != Versions.MATCH_DELETED) {
                e = addValidationError("unsupported version for internal versioning [" + destination.version() + ']', e);
            }
        }
        if (getRemoteInfo() != null) {
            if (getSearchRequest().source().query() != null) {
                e = addValidationError("reindex from remote sources should use RemoteInfo's query instead of source's query", e);
            }
            if (getSlices() == AbstractBulkByScrollRequest.AUTO_SLICES || getSlices() > 1) {
                e = addValidationError("reindex from remote sources doesn't support slices > 1 but was [" + getSlices() + "]", e);
            }
        }
        return e;
    }

    private boolean routingIsValid() {
        if (destination.routing() == null || destination.routing().startsWith("=")) {
            return true;
        }
        switch (destination.routing()) {
        case "keep":
        case "discard":
            return true;
        default:
            return false;
        }
    }

    /**
     * Set the indices which will act as the source for the ReindexRequest
     */
    public ReindexRequest setSourceIndices(String... sourceIndices) {
        if (sourceIndices != null) {
            this.getSearchRequest().indices(sourceIndices);
        }
        return this;
    }

    /**
     * Set the document types which need to be copied from the source indices
     */
    public ReindexRequest setSourceDocTypes(String... docTypes) {
        if (docTypes != null) {
            this.getSearchRequest().types(docTypes);
        }
        return this;
    }

    /**
     * Sets the scroll size for setting how many documents are to be processed in one batch during reindex
     */
    public ReindexRequest setSourceBatchSize(int size) {
        this.getSearchRequest().source().size(size);
        return this;
    }

    /**
     * Set the query for selecting documents from the source indices
     */
    public ReindexRequest setSourceQuery(QueryBuilder queryBuilder) {
        if (queryBuilder != null) {
            this.getSearchRequest().source().query(queryBuilder);
        }
        return this;
    }

    /**
     * Add a sort against the given field name.
     *
     * @param name The name of the field to sort by
     * @param order The order in which to sort
     */
    public ReindexRequest addSortField(String name, SortOrder order) {
        this.getSearchRequest().source().sort(name, order);
        return this;
    }

    /**
     * Set the target index for the ReindexRequest
     */
    public ReindexRequest setDestIndex(String destIndex) {
        if (destIndex != null) {
            this.getDestination().index(destIndex);
        }
        return this;
    }

    /**
     * Set the document type for the destination index
     */
    public ReindexRequest setDestDocType(String docType) {
        this.getDestination().type(docType);
        return this;
    }

    /**
     * Set the routing to decide which shard the documents need to be routed to
     */
    public ReindexRequest setDestRouting(String routing) {
        this.getDestination().routing(routing);
        return this;
    }

    /**
     * Set the version type for the target index. A {@link VersionType#EXTERNAL} helps preserve the version
     * if the document already existed in the target index.
     */
    public ReindexRequest setDestVersionType(VersionType versionType) {
        this.getDestination().versionType(versionType);
        return this;
    }

    /**
     * Allows to set the ingest pipeline for the target index.
     */
    public void setDestPipeline(String pipelineName) {
        this.getDestination().setPipeline(pipelineName);
    }

    /**
     * Sets the optype on the destination index
     * @param opType must be one of {create, index}
     */
    public ReindexRequest setDestOpType(String opType) {
        this.getDestination().opType(opType);
        return this;
    }

    /**
     * Set the {@link RemoteInfo} if the source indices are in a remote cluster.
     */
    public ReindexRequest setRemoteInfo(RemoteInfo remoteInfo) {
        this.remoteInfo = remoteInfo;
        return this;
    }

    /**
     * Gets the target for this reindex request in the for of an {@link IndexRequest}
     */
    public IndexRequest getDestination() {
        return destination;
    }

    /**
     * Get the {@link RemoteInfo} if it was set for this request.
     */
    public RemoteInfo getRemoteInfo() {
        return remoteInfo;
    }

    @Override
    public ReindexRequest forSlice(TaskId slicingTask, SearchRequest slice, int totalSlices) {
        ReindexRequest sliced = doForSlice(new ReindexRequest(slice, destination, false), slicingTask, totalSlices);
        sliced.setRemoteInfo(remoteInfo);
        return sliced;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        destination.writeTo(out);
        out.writeOptionalWriteable(remoteInfo);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("reindex from ");
        if (remoteInfo != null) {
            b.append('[').append(remoteInfo).append(']');
        }
        searchToString(b);
        b.append(" to [").append(destination.index()).append(']');
        if (destination.type() != null) {
            b.append('[').append(destination.type()).append(']');
        }
        return b.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            // build source
            builder.startObject("source");
            if (remoteInfo != null) {
                builder.field("remote", remoteInfo);
            }
            builder.array("index", getSearchRequest().indices());
            builder.array("type", getSearchRequest().types());
            getSearchRequest().source().innerToXContent(builder, params);
            builder.endObject();
        }
        {
            // build destination
            builder.startObject("dest");
            builder.field("index", getDestination().index());
            if (getDestination().type() != null) {
                builder.field("type", getDestination().type());
            }
            if (getDestination().routing() != null) {
                builder.field("routing", getDestination().routing());
            }
            builder.field("op_type", getDestination().opType().getLowercase());
            if (getDestination().getPipeline() != null) {
                builder.field("pipeline", getDestination().getPipeline());
            }
            builder.field("version_type", VersionType.toString(getDestination().versionType()));
            builder.endObject();
        }
        {
            // Other fields
            if (getSize() != -1 || getSize() > 0) {
                builder.field("size", getSize());
            }
            if (getScript() != null) {
                builder.field("script", getScript());
            }
            if (isAbortOnVersionConflict() == false) {
                builder.field("conflicts", "proceed");
            }
        }
        builder.endObject();
        return builder;
    }
}
