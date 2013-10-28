package org.cratedb.action.parser;

import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.parser.parser.NodeTypes;
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

import java.util.Set;

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
        if (stmt.nodeType() == NodeTypes.UPDATE_NODE) {
            request.preference("_primary");
        }

        return request;
    }

    public CountRequest buildCountRequest() {
        CountRequest request = new CountRequest();
        request.indices(stmt.indices());
        request.query(stmt.xcontent.toBytes());
        return request;
    }

    public IndexRequest buildIndexRequest() {
        IndexRequest[] requests = stmt.indexRequests;
        assert requests.length == 1;
        return requests[0];
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
            stmt.tableName(), NodeExecutionContext.DEFAULT_TYPE, stmt.primaryKeyLookupValue);
        request.fields(stmt.columnNames());
        request.realtime(true);
        return request;
    }

    public MultiGetRequest buildMultiGetRequest() {
        Set<String> ids = stmt.primaryKeyValues;
        assert ids != null;
        MultiGetRequest request = new MultiGetRequest();
        for (String id: ids) {
            MultiGetRequest.Item item
                = new MultiGetRequest.Item(stmt.tableName(), NodeExecutionContext.DEFAULT_TYPE, id);
            item.fields(stmt.columnNames());
            request.add(item);
        }
        request.realtime(true);
        return request;
    }

    public DeleteByQueryRequest buildDeleteByQueryRequest() {
        DeleteByQueryRequest request = new DeleteByQueryRequest();
        request.query(stmt.xcontent.toBytes());
        request.indices(stmt.indices());

        // // Set routing values if found by planner
        if (stmt.routingValues != null && !stmt.routingValues.isEmpty()) {
            request.routing(stmt.getRoutingValues());
        }

        return request;
    }


    public DeleteRequest buildDeleteRequest() {
        DeleteRequest request = new DeleteRequest(
            stmt.tableName(), NodeExecutionContext.DEFAULT_TYPE, stmt.primaryKeyLookupValue);

        // Set version if found by planner
        if (stmt.versionFilter != null) {
            request.version(stmt.versionFilter);
        }

        return request;
    }

    public UpdateRequest buildUpdateRequest() {
        UpdateRequest request = new UpdateRequest(
            stmt.tableName(), NodeExecutionContext.DEFAULT_TYPE, stmt.primaryKeyLookupValue);
        request.fields(stmt.cols());
        request.doc(stmt.updateDoc());
        request.retryOnConflict(ParsedStatement.UPDATE_RETRY_ON_CONFLICT);

        return request;
    }

     public CreateIndexRequest buildCreateIndexRequest() {
         CreateIndexRequest request = new CreateIndexRequest(stmt.tableName());
         request.settings(stmt.indexSettings);
         request.mapping(NodeExecutionContext.DEFAULT_TYPE, stmt.indexMapping);

         return request;
     }

     public DeleteIndexRequest buildDeleteIndexRequest() {
         return new DeleteIndexRequest(stmt.tableName());
     }
}
