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

package org.elasticsearch.action.update;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.DocumentSourceMissingException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Helper for translating an update request to an index, delete request or update response.
 */
public class UpdateHelper extends AbstractComponent {

    public UpdateHelper(Settings settings) {
        super(settings);
    }

    /**
     * Prepares an update request by converting it into an index or delete request or an update response (no action).
     */
    public Result prepare(UpdateRequest request, IndexShard indexShard, LongSupplier nowInMillis) {
        final GetResult getResult = indexShard.getService().getForUpdate(request.type(), request.id(), request.version(),
            request.versionType());
        return prepare(indexShard.shardId(), request, getResult, nowInMillis);
    }

    /**
     * Prepares an update request by converting it into an index or delete request or an update response (no action, in the event of a
     * noop).
     */
    @SuppressWarnings("unchecked")
    protected Result prepare(ShardId shardId, UpdateRequest request, final GetResult getResult, LongSupplier nowInMillis) {
        if (getResult.isExists() == false) {
            // If the document didn't exist, execute the update request as an upsert
            return prepareUpsert(shardId, request, getResult, nowInMillis);
        } else if (getResult.internalSourceRef() == null) {
            // no source, we can't do anything, throw a failure...
            throw new DocumentSourceMissingException(shardId, request.type(), request.id());
        } else if (request.doc() != null) {
            // The request is a new doc that should be merged with the old document
            return prepareUpdateIndexRequest(shardId, request, getResult, request.detectNoop());
        } else {
            throw new IllegalArgumentException("doc is missing");
        }
    }

    /**
     * Prepare the request for upsert, executing the upsert script if present, and returning a {@code Result} containing a new
     * {@code IndexRequest} to be executed on the primary and replicas.
     */
    Result prepareUpsert(ShardId shardId, UpdateRequest request, final GetResult getResult, LongSupplier nowInMillis) {
            if (request.upsertRequest() == null && !request.docAsUpsert()) {
                throw new DocumentMissingException(shardId, request.type(), request.id());
            }
            IndexRequest indexRequest = request.docAsUpsert() ? request.doc() : request.upsertRequest();

            indexRequest.index(request.index())
                    .type(request.type()).id(request.id()).setRefreshPolicy(request.getRefreshPolicy()).routing(request.routing())
                    .parent(request.parent()).timeout(request.timeout()).waitForActiveShards(request.waitForActiveShards())
                    // it has to be a "create!"
                    .create(true);

            if (request.versionType() != VersionType.INTERNAL) {
                // in all but the internal versioning mode, we want to create the new document using the given version.
                indexRequest.version(request.version()).versionType(request.versionType());
            }

            return new Result(indexRequest, DocWriteResponse.Result.CREATED, null, null);
    }

    /**
     * Calculate the version to use for the update request, using either the existing version if internal versioning is used, or the get
     * result document's version if the version type is "FORCE".
     */
    static long calculateUpdateVersion(UpdateRequest request, GetResult getResult) {
        if (request.versionType() != VersionType.INTERNAL) {
            assert request.versionType() == VersionType.FORCE;
            return request.version(); // remember, match_any is excluded by the conflict test
        } else {
            return getResult.getVersion();
        }
    }

    /**
     * Calculate a routing value to be used, either the included index request's routing, or retrieved document's routing when defined.
     */
    @Nullable
    static String calculateRouting(GetResult getResult, @Nullable IndexRequest updateIndexRequest) {
        if (updateIndexRequest != null && updateIndexRequest.routing() != null) {
            return updateIndexRequest.routing();
        } else if (getResult.getFields().containsKey(RoutingFieldMapper.NAME)) {
            return getResult.field(RoutingFieldMapper.NAME).getValue().toString();
        } else {
            return null;
        }
    }

    /**
     * Calculate a parent value to be used, either the included index request's parent, or retrieved document's parent when defined.
     */
    @Nullable
    static String calculateParent(GetResult getResult, @Nullable IndexRequest updateIndexRequest) {
        if (updateIndexRequest != null && updateIndexRequest.parent() != null) {
            return updateIndexRequest.parent();
        } else if (getResult.getFields().containsKey(ParentFieldMapper.NAME)) {
            return getResult.field(ParentFieldMapper.NAME).getValue().toString();
        } else {
            return null;
        }
    }

    /**
     * Prepare the request for merging the existing document with a new one, can optionally detect a noop change. Returns a {@code Result}
     * containing a new {@code IndexRequest} to be executed on the primary and replicas.
     */
    Result prepareUpdateIndexRequest(ShardId shardId, UpdateRequest request, GetResult getResult, boolean detectNoop) {
        final long updateVersion = calculateUpdateVersion(request, getResult);
        final IndexRequest currentRequest = request.doc();
        final String routing = calculateRouting(getResult, currentRequest);
        final String parent = calculateParent(getResult, currentRequest);
        final Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(getResult.internalSourceRef(), true);
        final XContentType updateSourceContentType = sourceAndContent.v1();
        final Map<String, Object> updatedSourceAsMap = sourceAndContent.v2();

        final boolean noop = !XContentHelper.update(updatedSourceAsMap, currentRequest.sourceAsMap(), detectNoop);

        // We can only actually turn the update into a noop if detectNoop is true to preserve backwards compatibility and to handle cases
        // where users repopulating multi-fields or adding synonyms, etc.
        if (detectNoop && noop) {
            UpdateResponse update = new UpdateResponse(shardId, getResult.getType(), getResult.getId(),
                    getResult.getVersion(), DocWriteResponse.Result.NOOP);
            update.setGetResult(extractGetResult(request, request.index(), getResult.getVersion(), updatedSourceAsMap,
                            updateSourceContentType, getResult.internalSourceRef()));
            return new Result(update, DocWriteResponse.Result.NOOP, updatedSourceAsMap, updateSourceContentType);
        } else {
            final IndexRequest finalIndexRequest = Requests.indexRequest(request.index())
                    .type(request.type()).id(request.id()).routing(routing).parent(parent)
                    .source(updatedSourceAsMap, updateSourceContentType).version(updateVersion).versionType(request.versionType())
                    .waitForActiveShards(request.waitForActiveShards()).timeout(request.timeout())
                    .setRefreshPolicy(request.getRefreshPolicy());
            return new Result(finalIndexRequest, DocWriteResponse.Result.UPDATED, updatedSourceAsMap, updateSourceContentType);
        }
    }

    /**
     * Applies {@link UpdateRequest#fetchSource()} to the _source of the updated document to be returned in a update response.
     * For BWC this function also extracts the {@link UpdateRequest#fields()} from the updated document to be returned in a update response
     */
    public static GetResult extractGetResult(final UpdateRequest request, String concreteIndex, long version,
                                             final Map<String, Object> source, XContentType sourceContentType,
                                             @Nullable final BytesReference sourceAsBytes) {
        if ((request.fields() == null || request.fields().length == 0) &&
            (request.fetchSource() == null || request.fetchSource().fetchSource() == false)) {
            return null;
        }
        SourceLookup sourceLookup = new SourceLookup();
        sourceLookup.setSource(source);
        boolean sourceRequested = false;
        Map<String, DocumentField> fields = null;
        if (request.fields() != null && request.fields().length > 0) {
            for (String field : request.fields()) {
                if (field.equals("_source")) {
                    sourceRequested = true;
                    continue;
                }
                Object value = sourceLookup.extractValue(field);
                if (value != null) {
                    if (fields == null) {
                        fields = new HashMap<>(2);
                    }
                    DocumentField documentField = fields.get(field);
                    if (documentField == null) {
                        documentField = new DocumentField(field, new ArrayList<>(2));
                        fields.put(field, documentField);
                    }
                    documentField.getValues().add(value);
                }
            }
        }

        BytesReference sourceFilteredAsBytes = sourceAsBytes;
        if (request.fetchSource() != null && request.fetchSource().fetchSource()) {
            sourceRequested = true;
            if (request.fetchSource().includes().length > 0 || request.fetchSource().excludes().length > 0) {
                Object value = sourceLookup.filter(request.fetchSource());
                try {
                    final int initialCapacity = Math.min(1024, sourceAsBytes.length());
                    BytesStreamOutput streamOutput = new BytesStreamOutput(initialCapacity);
                    try (XContentBuilder builder = new XContentBuilder(sourceContentType.xContent(), streamOutput)) {
                        builder.value(value);
                        sourceFilteredAsBytes = BytesReference.bytes(builder);
                    }
                } catch (IOException e) {
                    throw new ElasticsearchException("Error filtering source", e);
                }
            }
        }

        // TODO when using delete/none, we can still return the source as bytes by generating it (using the sourceContentType)
        return new GetResult(concreteIndex, request.type(), request.id(), version, true,
                sourceRequested ? sourceFilteredAsBytes : null, fields);
    }

    public static class Result {

        private final Streamable action;
        private final DocWriteResponse.Result result;
        private final Map<String, Object> updatedSourceAsMap;
        private final XContentType updateSourceContentType;

        public Result(Streamable action, DocWriteResponse.Result result, Map<String, Object> updatedSourceAsMap,
                      XContentType updateSourceContentType) {
            this.action = action;
            this.result = result;
            this.updatedSourceAsMap = updatedSourceAsMap;
            this.updateSourceContentType = updateSourceContentType;
        }

        @SuppressWarnings("unchecked")
        public <T extends Streamable> T action() {
            return (T) action;
        }

        public DocWriteResponse.Result getResponseResult() {
            return result;
        }

        public Map<String, Object> updatedSourceAsMap() {
            return updatedSourceAsMap;
        }

        public XContentType updateSourceContentType() {
            return updateSourceContentType;
        }
    }

    /**
     * After executing the script, this is the type of operation that will be used for subsequent actions. This corresponds to the "ctx.op"
     * variable inside of scripts.
     */
    enum UpdateOpType {
        CREATE("create"),
        INDEX("index"),
        DELETE("delete"),
        NONE("none");

        private final String name;

        UpdateOpType(String name) {
            this.name = name;
        }

        public static UpdateOpType lenientFromString(String operation, Logger logger) {
            switch (operation) {
                case "create":
                    return UpdateOpType.CREATE;
                case "index":
                    return UpdateOpType.INDEX;
                case "delete":
                    return UpdateOpType.DELETE;
                default:
                case "none":
                    return UpdateOpType.NONE;
            }
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Field names used to populate the script context
     */
    public static class ContextFields {
        public static final String CTX = "ctx";
        public static final String OP = "op";
        public static final String SOURCE = "_source";
        public static final String NOW = "_now";
        public static final String INDEX = "_index";
        public static final String TYPE = "_type";
        public static final String ID = "_id";
        public static final String VERSION = "_version";
        public static final String ROUTING = "_routing";
        public static final String PARENT = "_parent";
    }
}
