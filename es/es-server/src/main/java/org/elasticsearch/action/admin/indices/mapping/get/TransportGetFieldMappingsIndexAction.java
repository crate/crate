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

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetaData;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentFieldMappers;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;

/**
 * Transport action used to retrieve the mappings related to fields that belong to a specific index
 */
public class TransportGetFieldMappingsIndexAction extends TransportSingleShardAction<GetFieldMappingsIndexRequest, GetFieldMappingsResponse> {

    private static final String ACTION_NAME = GetFieldMappingsAction.NAME + "[index]";

    protected final ClusterService clusterService;
    private final IndicesService indicesService;

    @Inject
    public TransportGetFieldMappingsIndexAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                                IndicesService indicesService, ThreadPool threadPool, ActionFilters actionFilters,
                                                IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                GetFieldMappingsIndexRequest::new, ThreadPool.Names.MANAGEMENT);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    @Override
    protected boolean resolveIndex(GetFieldMappingsIndexRequest request) {
        //internal action, index already resolved
        return false;
    }

    @Override
    protected ShardsIterator shards(ClusterState state, InternalRequest request) {
        // Will balance requests between shards
        return state.routingTable().index(request.concreteIndex()).randomAllActiveShardsIt();
    }

    @Override
    protected GetFieldMappingsResponse shardOperation(final GetFieldMappingsIndexRequest request, ShardId shardId) {
        assert shardId != null;
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        Predicate<String> metadataFieldPredicate = indicesService::isMetaDataField;
        Predicate<String> fieldPredicate = metadataFieldPredicate.or(indicesService.getFieldFilter().apply(shardId.getIndexName()));

        Collection<String> typeIntersection;
        if (request.types().length == 0) {
            typeIntersection = indexService.mapperService().types();

        } else {
            typeIntersection = indexService.mapperService().types()
                    .stream()
                    .filter(type -> Regex.simpleMatch(request.types(), type))
                    .collect(Collectors.toCollection(ArrayList::new));
            if (typeIntersection.isEmpty()) {
                throw new TypeMissingException(shardId.getIndex(), request.types());
            }
        }

        Map<String, Map<String, FieldMappingMetaData>> typeMappings = new HashMap<>();
        for (String type : typeIntersection) {
            DocumentMapper documentMapper = indexService.mapperService().documentMapper(type);
            Map<String, FieldMappingMetaData> fieldMapping = findFieldMappingsByType(fieldPredicate, documentMapper, request);
            if (!fieldMapping.isEmpty()) {
                typeMappings.put(type, fieldMapping);
            }
        }
        return new GetFieldMappingsResponse(singletonMap(shardId.getIndexName(), Collections.unmodifiableMap(typeMappings)));
    }

    @Override
    protected GetFieldMappingsResponse newResponse() {
        return new GetFieldMappingsResponse();
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_READ, request.concreteIndex());
    }

    private static final ToXContent.Params includeDefaultsParams = new ToXContent.Params() {

        static final String INCLUDE_DEFAULTS = "include_defaults";

        @Override
        public String param(String key) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return "true";
            }
            return null;
        }

        @Override
        public String param(String key, String defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return "true";
            }
            return defaultValue;
        }

        @Override
        public boolean paramAsBoolean(String key, boolean defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return true;
            }
            return defaultValue;
        }

        @Override
        public Boolean paramAsBoolean(String key, Boolean defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return true;
            }
            return defaultValue;
        }
    };

    private static Map<String, FieldMappingMetaData> findFieldMappingsByType(Predicate<String> fieldPredicate,
                                                                             DocumentMapper documentMapper,
                                                                             GetFieldMappingsIndexRequest request) {
        Map<String, FieldMappingMetaData> fieldMappings = new HashMap<>();
        final DocumentFieldMappers allFieldMappers = documentMapper.mappers();
        for (String field : request.fields()) {
            if (Regex.isMatchAllPattern(field)) {
                for (Mapper fieldMapper : allFieldMappers) {
                    addFieldMapper(fieldPredicate, fieldMapper.name(), fieldMapper, fieldMappings, request.includeDefaults());
                }
            } else if (Regex.isSimpleMatchPattern(field)) {
                for (Mapper fieldMapper : allFieldMappers) {
                    if (Regex.simpleMatch(field, fieldMapper.name())) {
                        addFieldMapper(fieldPredicate,  fieldMapper.name(),
                                fieldMapper, fieldMappings, request.includeDefaults());
                    }
                }
            } else {
                // not a pattern
                Mapper fieldMapper = allFieldMappers.getMapper(field);
                if (fieldMapper != null) {
                    addFieldMapper(fieldPredicate, field, fieldMapper, fieldMappings, request.includeDefaults());
                } else if (request.probablySingleFieldRequest()) {
                    fieldMappings.put(field, FieldMappingMetaData.NULL);
                }
            }
        }
        return Collections.unmodifiableMap(fieldMappings);
    }

    private static void addFieldMapper(Predicate<String> fieldPredicate,
                                       String field, Mapper fieldMapper, Map<String, FieldMappingMetaData> fieldMappings,
                                       boolean includeDefaults) {
        if (fieldMappings.containsKey(field)) {
            return;
        }
        if (fieldPredicate.test(field)) {
            try {
                BytesReference bytes = XContentHelper.toXContent(fieldMapper, XContentType.JSON,
                        includeDefaults ? includeDefaultsParams : ToXContent.EMPTY_PARAMS, false);
                fieldMappings.put(field, new FieldMappingMetaData(fieldMapper.name(), bytes));
            } catch (IOException e) {
                throw new ElasticsearchException("failed to serialize XContent of field [" + field + "]", e);
            }
        }
    }
}
