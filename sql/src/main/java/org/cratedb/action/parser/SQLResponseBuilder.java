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

import org.cratedb.action.import_.ImportResponse;
import org.cratedb.action.import_.NodeImportResponse;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLFields;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.import_.Importer;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.ExceptionHelper;
import org.cratedb.sql.facet.InternalSQLFacet;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;

/**
 * The SQLResponseBuilder can be used to convert ES Responses into a {@link SQLResponse}
 */
public class SQLResponseBuilder {

    private final ParsedStatement stmt;
    private final NodeExecutionContext context;

    public SQLResponseBuilder(NodeExecutionContext context, ParsedStatement stmt) {
        this.context = context;
        this.stmt = stmt;
    }

    public SQLResponse buildResponse(IndexResponse indexResponse, long requestStartedTime) {
        return buildEmptyResponse(1, requestStartedTime);
    }

    public SQLResponse buildResponse(BulkResponse bulkResponse, long requestStartedTime) {
        // Pseudo row count by counting non-failed responses
        // This assumes one document was hit by each request, which is only true for e.g.
        // multiple IndexRequests.
        BulkItemResponse[] responses = bulkResponse.getItems();
        int rowsAffected = 0;
        for (BulkItemResponse response : responses) {
            if (!response.isFailed()) {
                rowsAffected++;
            }
        }
        return buildEmptyResponse(rowsAffected, requestStartedTime);
    }

    public SQLResponse buildResponse(GetResponse getResponse, long requestStartedTime) {
        if (! getResponse.isExists()) {
            return buildEmptyResponse(0, requestStartedTime);
        }

        SQLResponse response = new SQLResponse();
        SQLFields fields = new SQLFields(stmt.outputFields);
        Object[][] rows = new Object[1][stmt.outputFields.size()];

        // only works with one queried index/table
        fields.applyGetResponse(context.tableContext(stmt.schemaName(), stmt.tableName()), getResponse);
        rows[0] = fields.getRowValues();

        response.cols(stmt.cols());
        response.rows(rows);
        response.rowCount(1);
        response.requestStartedTime(requestStartedTime);

        return response;
    }

    public SQLResponse buildResponse(MultiGetResponse multiGetItemResponses,
                                     long requestStartedTime) {
        SQLResponse response = new SQLResponse();
        SQLFields fields = new SQLFields(stmt.outputFields);
        List<Object[]> rows = new ArrayList<>();
        MultiGetItemResponse[] singleResponses = multiGetItemResponses.getResponses();
        long successful = 0;
        for (int i=0; i < singleResponses.length; i++) {
            if (!singleResponses[i].isFailed()) {
                if (singleResponses[i].getResponse().isExists()) {
                    fields.applyGetResponse(
                        context.tableContext(stmt.schemaName(), stmt.tableName()), singleResponses[i].getResponse());
                    rows.add(fields.getRowValues());
                    successful++;
                }
            } else {
                // failure on at least one shard
                throw new CrateException(singleResponses[i].getFailure().getMessage());
            }
        }
        response.cols(stmt.cols());
        response.rows(rows.toArray(new Object[rows.size()][stmt.outputFields.size()]));
        response.rowCount(successful);
        response.requestStartedTime(requestStartedTime);
        return response;
    }

    public SQLResponse buildResponse(DeleteByQueryResponse deleteByQueryResponse,
                                     long requestStartedTime) {
        // TODO: add rows affected
        return buildEmptyResponse(-1, requestStartedTime);
    }

    public SQLResponse buildResponse(DeleteResponse deleteResponse, long requestStartedTime) {
        int rowCount = 0;
        if (! deleteResponse.isNotFound()) {
            rowCount = 1;
        }

        return buildEmptyResponse(rowCount, requestStartedTime);
    }

    public SQLResponse buildResponse(UpdateResponse updateResponse, long requestStartedTime) {
        return buildEmptyResponse(1, requestStartedTime);
    }

    public SQLResponse buildResponse(CreateIndexResponse createIndexResponse,
                                     long requestStartedTime) {
        return buildEmptyResponse(0, requestStartedTime);
    }

    public SQLResponse buildResponse(DeleteIndexResponse deleteIndexResponse,
                                     long requestStartedTime) {
        return buildEmptyResponse(0, requestStartedTime);
    }

    public SQLResponse buildMissingDocumentResponse(long requestStartedTime) {
        return buildEmptyResponse(0, requestStartedTime);
    }

    private SQLResponse buildEmptyResponse(int rowCount, long requestStartedTime) {
        SQLResponse response = new SQLResponse();
        response.cols(stmt.cols());
        response.rows(new Object[0][0]);
        response.rowCount(rowCount);
        response.requestStartedTime(requestStartedTime);

        return response;
    }


    public SQLResponse buildResponse(SearchResponse searchResponse,
                                     InternalSQLFacet facet,
                                     long requestStartedTime) {
        facet.reduce(stmt);
        return new SQLResponse(stmt.cols(), facet.rows(), facet.rowCount(), requestStartedTime);
    }

    public SQLResponse buildResponse(CountResponse countResponse, long requestStartedTime) {
        Object[][] rows = new Object[1][];
        rows[0] = new Object[] { countResponse.getCount() };
        return new SQLResponse(stmt.cols(), rows, rows.length, requestStartedTime);
    }

    public SQLResponse buildResponse(SearchResponse searchResponse, long requestStartedTime) {

        if (searchResponse.getFailedShards() > 0) {
            ExceptionHelper.exceptionOnSearchShardFailures(searchResponse.getShardFailures());
        }

        if (stmt.useFacet()){
            return buildResponse(searchResponse,
                    searchResponse.getFacets().facet(InternalSQLFacet.class, "sql"),
                    requestStartedTime);
        }
        SQLFields fields = new SQLFields(stmt.outputFields);
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        Object[][] rows = new Object[searchHits.length][stmt.outputFields.size()];


        for (int i = 0; i < searchHits.length; i++) {
            SearchHit hit = searchHits[i];
            fields.hit(hit);
            rows[i] = fields.getRowValues();
        }

        SQLResponse response = new SQLResponse();
        response.cols(stmt.cols());
        response.rows(rows);
        response.rowCount(rows.length);
        response.requestStartedTime(requestStartedTime);
        return response;
    }

    public SQLResponse buildResponse(ClusterUpdateSettingsResponse clusterUpdateSettingsResponse,
                                     long requestStartedTime) {
        return buildEmptyResponse(0, requestStartedTime);
    }

    public SQLResponse buildResponse(ImportResponse importResponse, long requestStartedTime) {
        int rowCount = 0;
        for (NodeImportResponse nodeImportResponse : importResponse.getResponses()) {
            for (Importer.ImportCounts importCounts : nodeImportResponse.result().importCounts) {
                rowCount += importCounts.successes;
            }
        }
        return buildEmptyResponse(rowCount, requestStartedTime);
    }

}
