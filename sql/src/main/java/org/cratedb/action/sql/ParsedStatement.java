package org.cratedb.action.sql;

import org.cratedb.action.parser.InsertVisitor;
import org.cratedb.action.parser.QueryPlanner;
import org.cratedb.action.parser.QueryVisitor;
import org.cratedb.action.parser.XContentVisitor;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.VersionConflictException;
import org.cratedb.sql.facet.InternalSQLFacet;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.NodeTypes;
import org.cratedb.sql.parser.parser.SQLParser;
import org.cratedb.sql.parser.parser.StatementNode;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParsedStatement {

    private XContentBuilder builder;

    private final ArrayList<Tuple<String, String>> outputFields =
            new ArrayList<>();
    private final NodeExecutionContext context;
    private final List<String> indices = new ArrayList<>(1);
    private ESLogger logger = Loggers.getLogger(ParsedStatement.class);

    private final String stmt;
    private final Object[] args;
    private final StatementNode statementNode;
    private final XContentVisitor visitor;

    public static final int SEARCH_ACTION = 1;
    public static final int INSERT_ACTION = 2;
    public static final int DELETE_BY_QUERY_ACTION = 3;
    public static final int BULK_ACTION = 4;
    public static final int GET_ACTION = 5;
    public static final int DELETE_ACTION = 6;
    public static final int UPDATE_ACTION = 7;

    public static final int UPDATE_RETRY_ON_CONFLICT = 3;

    private NodeExecutionContext.TableExecutionContext tableContext;
    private Map<String, Object> updateDoc;
    private Map<String, Object> plannerResults;
    private boolean countRequest;

    public ParsedStatement(String stmt, Object[] args, NodeExecutionContext context) throws
            StandardException {
        this.stmt = stmt;
        this.args = args;
        this.context = context;
        this.tableContext = null;
        this.plannerResults = new HashMap<>();
        SQLParser parser = new SQLParser();
        statementNode = parser.parseStatement(stmt);
        switch (statementNode.getNodeType()) {
            case NodeTypes.INSERT_NODE:
                visitor = new InsertVisitor(this);
                break;
            default:
                visitor = new QueryVisitor(this);
                break;
        }
        statementNode.accept(visitor);
    }

    public boolean addIndex(String index){
        if (indices.contains(index)){
            return false;
        }
        return indices.add(index);
    }

    public NodeExecutionContext context(){
        return context;
    }

    public void tableContext(NodeExecutionContext.TableExecutionContext tableContext) {
        this.tableContext = tableContext;
    }

    public NodeExecutionContext.TableExecutionContext tableContext() {
        return tableContext;
    }

    public int type() {
        switch (statementNode.getNodeType()) {
            case NodeTypes.INSERT_NODE:
                if (((InsertVisitor)visitor).isBulk()) {
                    return BULK_ACTION;
                }
                return INSERT_ACTION;
            case NodeTypes.DELETE_NODE:
                if (getPlannerResult(QueryPlanner.PRIMARY_KEY_VALUE) != null) {
                    return DELETE_ACTION;
                }
                return DELETE_BY_QUERY_ACTION;
            case NodeTypes.CURSOR_NODE:
                if (getPlannerResult(QueryPlanner.PRIMARY_KEY_VALUE) != null) {
                    return GET_ACTION;
                }
                return SEARCH_ACTION;
            case NodeTypes.UPDATE_NODE:
                if (getPlannerResult(QueryPlanner.PRIMARY_KEY_VALUE) != null) {
                    return UPDATE_ACTION;
                }
                return SEARCH_ACTION;
            default:
                return SEARCH_ACTION;
        }
    }

    public SearchRequest buildSearchRequest() throws StandardException {
        SearchRequest request = new SearchRequest();
        builder = visitor.getXContentBuilder();

        if (logger.isDebugEnabled()) {
            builder.generator().usePrettyPrint();
            try {
                logger.info("converted sql to: " + builder.string());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        request.source(builder.bytes().toBytes());
        request.indices(indices.toArray(new String[indices.size()]));
        return request;
    }

    public CountRequest buildCountRequest() throws StandardException {
        CountRequest request = new CountRequest();
        builder = visitor.getXContentBuilder();
        request.indices(indices.toArray(new String[indices.size()]));
        request.query(builder.bytes().toBytes());
        return request;
    }

    public IndexRequest buildIndexRequest() throws StandardException {

        InsertVisitor insertVisitor = (InsertVisitor)visitor;
        IndexRequest[] requests = insertVisitor.indexRequests();
        assert requests.length == 1;
        return requests[0];
    }

    public BulkRequest buildBulkRequest() throws Exception {
        BulkRequest request = new BulkRequest();

        if (logger.isDebugEnabled()) {
            builder.generator().usePrettyPrint();
            try {
                logger.info("converted sql to: " + builder.string());
            } catch (IOException e) {
                logger.error("Error while converting json to string for debugging", e);
            }
        }

        InsertVisitor insertVisitor = (InsertVisitor)visitor;
        for (IndexRequest indexRequest : insertVisitor.indexRequests()) {
            request.add(indexRequest);
        }

        return request;
    }

    public GetRequest buildGetRequest() {
        String id = (String)getPlannerResult(QueryPlanner.PRIMARY_KEY_VALUE);
        GetRequest request = new GetRequest(indices.get(0),
                NodeExecutionContext.DEFAULT_TYPE, id);
        request.routing(id);
        request.fields(cols());
        request.realtime(true);
        return request;
    }

    public DeleteRequest buildDeleteRequest() {
        String id = (String)getPlannerResult(QueryPlanner.PRIMARY_KEY_VALUE);
        DeleteRequest request = new DeleteRequest(indices.get(0),
                NodeExecutionContext.DEFAULT_TYPE, id);
        request.routing(id);

        return request;
    }

    public UpdateRequest buildUpdateRequest() {
        String id = (String)getPlannerResult(QueryPlanner.PRIMARY_KEY_VALUE);
        UpdateRequest request = new UpdateRequest(indices.get(0),
                NodeExecutionContext.DEFAULT_TYPE, id);
        request.routing(id);
        request.fields(cols());
        request.doc(updateDoc());
        request.retryOnConflict(UPDATE_RETRY_ON_CONFLICT);

        return request;
    }

    public String[] cols() {
        String[] cols = new String[outputFields.size()];
        for (int i = 0; i < outputFields.size(); i++) {
            cols[i] = outputFields.get(i).v1();
        }
        return cols;
    }

    public SQLResponse buildResponse(SearchResponse searchResponse, InternalSQLFacet facet) {
        facet.reduce(this);
        return new SQLResponse(cols(), facet.rows(), facet.rowCount());
    }

    public SQLResponse buildResponse(CountResponse countResponse) {
        Object[][] rows = new Object[1][];
        rows[0] = new Object[] { countResponse.getCount() };
        return new SQLResponse(cols(), rows, rows.length);
    }

    public SQLResponse buildResponse(SearchResponse searchResponse) {

        if (searchResponse.getFailedShards() > 0) {
            for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
                if (failure.failure().getCause() instanceof VersionConflictEngineException) {
                    throw new VersionConflictException(failure.failure());
                }
            }

            // just take the first failure to have at least some stack trace.
            throw new CrateException(searchResponse.getFailedShards() + " shard failures",
                searchResponse.getShardFailures()[0].failure());
        }

        if (useFacet()){
            return buildResponse(searchResponse,
                    searchResponse.getFacets().facet(InternalSQLFacet.class, "sql"));
        }
        SQLFields fields = new SQLFields(outputFields);
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        Object[][] rows = new Object[searchHits.length][outputFields.size()];


        for (int i = 0; i < searchHits.length; i++) {
            SearchHit hit = searchHits[i];
            fields.hit(hit);
            rows[i] = fields.getRowValues();
        }

        SQLResponse response = new SQLResponse();
        response.cols(cols());
        response.rows(rows);
        response.rowCount(rows.length);
        return response;
    }


    /**
     * @return boolean indicating if a facet is used to gather the result
     */
    private boolean useFacet() {
        // currently only the update statement uses facets
        return statementNode.getNodeType() == NodeTypes.UPDATE_NODE;
    }

    public SQLResponse buildResponse(IndexResponse indexResponse) {
        return buildEmptyResponse(1);
    }

    public SQLResponse buildResponse(BulkResponse bulkResponse) {
        // TODO: add rows affected
        return buildEmptyResponse(0);
    }

    public SQLResponse buildResponse(GetResponse getResponse) {

        if (! getResponse.isExists()) {
            return buildEmptyResponse(0);
        }

        SQLResponse response = new SQLResponse();
        SQLFields fields = new SQLFields(outputFields);
        Object[][] rows = new Object[1][outputFields.size()];

        // only works with one queried index/table
        fields.applyGetResponse(context().tableContext(indices().get(0)), getResponse);
        rows[0] = fields.getRowValues();

        response.cols(cols());
        response.rows(rows);
        response.rowCount(1);

        return response;
    }

    public SQLResponse buildResponse(DeleteByQueryResponse deleteByQueryResponse) {
        // TODO: add rows affected
        return buildEmptyResponse(0);
    }

    public DeleteByQueryRequest buildDeleteByQueryRequest() throws StandardException {
        DeleteByQueryRequest request = new DeleteByQueryRequest();
        builder = visitor.getXContentBuilder();
        request.query(builder.bytes().toBytes());
        request.indices(indices.toArray(new String[indices.size()]));

        return request;
    }


    public SQLResponse buildResponse(DeleteResponse deleteResponse) {
        int rowCount = 0;
        if (! deleteResponse.isNotFound()) {
            rowCount = 1;
        }

        return buildEmptyResponse(rowCount);
    }

    public SQLResponse buildResponse(UpdateResponse updateResponse) {
        return buildEmptyResponse(1);
    }

    public SQLResponse buildMissingDocumentResponse() {
        return buildEmptyResponse(0);
    }

    private SQLResponse buildEmptyResponse(int rowCount) {
        SQLResponse response = new SQLResponse();
        response.cols(cols());
        response.rows(new Object[0][0]);
        response.rowCount(rowCount);

        return response;
    }

    public Map<String, Object> updateDoc() {
        return updateDoc;
    }

    public Object[] args() {
        return args;
    }

    public boolean hasArgs(){
        return (args != null && args.length>0);
    }

    public String stmt() {
        return stmt;
    }

    /**
     * returns the requested output fields as a list of tuples where
     * the left side is the alias and the right side is the column name
     *
     * @return list of tuples
     */
    public List<Tuple<String, String>> outputFields() {
        return outputFields;
    }

    /**
     * Adds an additional output field
     * @param alias the name under which the field will show up in the result
     * @param columnName the name of the column the value comes from
     */
    public void addOutputField(String alias, String columnName) {
        this.outputFields.add(new Tuple<String, String>(alias, columnName));
    }

    public void updateDoc(Map<String, Object> updateDoc) {
        this.updateDoc = updateDoc;
    }

    public List<String> indices() {
        return indices;
    }

    public void countRequest(boolean countRequest) {
        this.countRequest = countRequest;
    }

    public boolean countRequest() {
        return countRequest;
    }

    public void setPlannerResult(String key, Object value) {
        plannerResults.put(key, value);
    }

    public Object getPlannerResult(String key) {
        return plannerResults.get(key);
    }

    public XContentVisitor visitor() {
        return visitor;
    }
}
