package org.cratedb.action.parser;

import org.cratedb.action.import_.ImportRequest;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.Constants;
import org.cratedb.sql.parser.parser.NodeTypes;
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
                = new MultiGetRequest.Item(stmt.tableName(), Constants.DEFAULT_MAPPING_TYPE, id);
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
        request.retryOnConflict(ParsedStatement.UPDATE_RETRY_ON_CONFLICT);

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
