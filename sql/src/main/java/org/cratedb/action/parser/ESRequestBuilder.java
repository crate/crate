/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.action.parser;

import org.cratedb.action.import_.ImportRequest;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.Constants;
import org.cratedb.sql.parser.parser.NodeType;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.fetch.source.FetchSourceContext;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * The ESRequestBuilder can be used to build all sorts of ES Request from a {@link ParsedStatement}
 */
public class ESRequestBuilder {

    private final ParsedStatement stmt;

    public ESRequestBuilder(ParsedStatement stmt) {
        this.stmt = stmt;
    }

    public SearchRequest buildSearchRequest() {
        SearchRequest request = new SearchRequest();

        request.source(stmt.xcontent.toBytes());
        request.indices(stmt.indices());

        // // Set routing values if found by planner
        if (stmt.routingValues != null && !stmt.routingValues.isEmpty()) {
            request.routing(stmt.getRoutingValues());
        }

        // Update request should only be executed on primary shards
        if (stmt.nodeType() == NodeType.UPDATE_NODE) {
            request.preference("_primary");
        }

        return request;
    }

    public CountRequest buildCountRequest() {
        CountRequest request = new CountRequest();
        request.indices(stmt.indices());
        request.source(stmt.xcontent.toBytes());
        return request;
    }

    public IndexRequest buildIndexRequest() {
        List<IndexRequest> requests = stmt.indexRequests;
        assert requests.size() == 1;
        return requests.get(0);
    }

    public BulkRequest buildBulkRequest() throws Exception {
        BulkRequest request = new BulkRequest();
        for (IndexRequest indexRequest : stmt.indexRequests) {
            request.add(indexRequest);
        }

        return request;
    }

    public GetRequest buildGetRequest() {
        GetRequest request = new GetRequest(
            stmt.tableName(), Constants.DEFAULT_MAPPING_TYPE, stmt.primaryKeyLookupValue);
        request.fetchSourceContext(new FetchSourceContext(stmt.columnNames()));
        request.realtime(true);
        return request;
    }

    public MultiGetRequest buildMultiGetRequest() {
        Set<String> ids = stmt.primaryKeyValues;
        assert ids != null;
        MultiGetRequest request = new MultiGetRequest();
        FetchSourceContext fetchSourceContext = new FetchSourceContext(stmt.columnNames());
        for (String id: ids) {
            MultiGetRequest.Item item
                = new MultiGetRequest.Item(stmt.tableName(), Constants.DEFAULT_MAPPING_TYPE, id);
            item.fetchSourceContext(fetchSourceContext);
            request.add(item);
        }
        request.realtime(true);
        return request;
    }

    public DeleteByQueryRequest buildDeleteByQueryRequest() {
        DeleteByQueryRequest request = new DeleteByQueryRequest();
        request.source(stmt.xcontent.toBytes());
        request.indices(stmt.indices());

        // // Set routing values if found by planner
        if (stmt.routingValues != null && !stmt.routingValues.isEmpty()) {
            request.routing(stmt.getRoutingValues());
        }

        return request;
    }


    public DeleteRequest buildDeleteRequest() {
        DeleteRequest request = new DeleteRequest(
            stmt.tableName(), Constants.DEFAULT_MAPPING_TYPE, stmt.primaryKeyLookupValue);

        // Set version if found by planner
        if (stmt.versionFilter != null) {
            request.version(stmt.versionFilter);
        }

        return request;
    }

    public UpdateRequest buildUpdateRequest() {
        UpdateRequest request = new UpdateRequest(
            stmt.tableName(), Constants.DEFAULT_MAPPING_TYPE, stmt.primaryKeyLookupValue);
        request.fields(stmt.cols());
        request.paths(stmt.updateDoc());
        if (stmt.versionFilter != null ) {
            request.version(stmt.versionFilter);
        } else {
            request.retryOnConflict(ParsedStatement.UPDATE_RETRY_ON_CONFLICT);
        }

        return request;
    }

    public CreateIndexRequest buildCreateIndexRequest() {
        CreateIndexRequest request = new CreateIndexRequest(stmt.tableName());
        request.settings(stmt.indexSettings);
        request.mapping(Constants.DEFAULT_MAPPING_TYPE, stmt.indexMapping);

        return request;
    }

    public DeleteIndexRequest buildDeleteIndexRequest() {
        return new DeleteIndexRequest(stmt.tableName());
    }

    /**
    * Used for setting custom analyzers
    * @return
    */
    public ClusterUpdateSettingsRequest buildClusterUpdateSettingsRequest() {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(stmt.createAnalyzerSettings);
        return request;
    }

    public ImportRequest buildImportRequest() {
        ImportRequest importRequest = new ImportRequest();

        BytesReference source = null;
        try {
            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
            source = jsonBuilder.startObject()
                    .field("path", stmt.importPath)
                    .endObject().bytes();
        } catch (IOException e) {
        }
        importRequest.source(source, false);

        importRequest.index(stmt.indices()[0]);
        importRequest.type(Constants.DEFAULT_MAPPING_TYPE);

        return importRequest;
    }
}
