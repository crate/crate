package org.cratedb.action.sql;

import org.cratedb.action.parser.*;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.ExceptionHelper;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.facet.InternalSQLFacet;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.NodeTypes;
import org.cratedb.sql.parser.parser.SQLParser;
import org.cratedb.sql.parser.parser.StatementNode;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
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
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.*;


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
    public static final int CREATE_INDEX_ACTION = 8;
    public static final int DELETE_INDEX_ACTION = 9;

    public static final int UPDATE_RETRY_ON_CONFLICT = 3;

    private NodeExecutionContext.TableExecutionContext tableContext;
    private Map<String, Object> updateDoc;
    private Map<String, Object> plannerResults;
    private boolean countRequest;

    public List<String> groupByColumnNames;
    public List<ColumnDescription> resultColumnList;

    public Integer limit = null;
    public Integer offset = null;
    public List<OrderByColumnIdx> orderByIndices;
    public OrderByColumnIdx[] orderByIndices() {
        if (orderByIndices != null) {
            return orderByIndices.toArray(new OrderByColumnIdx[orderByIndices.size()]);
        }

        return new OrderByColumnIdx[0];
    }

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
            case NodeTypes.CREATE_TABLE_NODE:
            case NodeTypes.DROP_TABLE_NODE:
                visitor = new TableVisitor(this);
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

    public String tableName() {
        return indices().get(0);
    }

    public NodeExecutionContext context(){
        return context;
    }

    /**
     * Access the tableContext for {@link #tableName()} lazily.
     * The tableName has to be set with {@link #addIndex(String)} before this method can be used.
     *
     * Note that for tables that use dynamic mapping without any explicit columns this will always
     * throw an exception.
     *
     * @return TableExecutionContext for the table {@link #tableName()}
     * @throws SQLParseException in case the TableExecutionContext couldn't be loaded.
     */
    public NodeExecutionContext.TableExecutionContext tableContextSafe() throws SQLParseException {
        if (tableContext == null) {
            assert tableName() != null;
            tableContext = context().tableContext(tableName());
            if (tableContext == null) {
                throw new SQLParseException("No table definition found for " + tableName());
            }
        }
        return tableContext;
    }

    /**
     * Same as {@link #tableContextSafe()} but doesn't throw an Exception if the tableContext
     * cannot be loaded.
     *
     * @return TableExecutionContext for the table {@link #tableName()}
     */
    public NodeExecutionContext.TableExecutionContext tableContext() {
        if (tableContext == null) {
            assert tableName() != null;
            tableContext = context().tableContext(tableName());
        }
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
            case NodeTypes.CREATE_TABLE_NODE:
                return CREATE_INDEX_ACTION;
            case NodeTypes.DROP_TABLE_NODE:
                return DELETE_INDEX_ACTION;
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

        // Set routing values if found by planner
        @SuppressWarnings("unchecked")
        Set<String> routingValues = (Set<String>)getPlannerResult(QueryPlanner.ROUTING_VALUES);
        if (routingValues != null && !routingValues.isEmpty()) {
            List<String> tmp = new ArrayList<>(routingValues.size());
            tmp.addAll(routingValues);
            request.routing(tmp.toArray(new String[tmp.size()]));
        }

        // Update request should only be executed on primary shards
        if (statementNode.getNodeType() == NodeTypes.UPDATE_NODE) {
            request.preference("_primary");
        }

        return request;
    }

    public BytesReference getXContentAsBytesRef() throws StandardException {
        return visitor.getXContentBuilder().bytes();
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

    public CreateIndexRequest buildCreateIndexRequest() {
        assert visitor instanceof TableVisitor;
        CreateIndexRequest request = new CreateIndexRequest(indices.get(0));
        TableVisitor tableVisitor = (TableVisitor)visitor;
        request.settings(tableVisitor.settings());
        request.mapping(NodeExecutionContext.DEFAULT_TYPE, tableVisitor.mapping());

        return request;
    }

    public DeleteIndexRequest buildDeleteIndexRequest() {
        assert visitor instanceof TableVisitor;
        DeleteIndexRequest request = new DeleteIndexRequest(indices.get(0));

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
            ExceptionHelper.exceptionOnSearchShardFailures(searchResponse.getShardFailures());
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
        fields.applyGetResponse(tableContext(), getResponse);
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

        // Set routing values if found by planner
        @SuppressWarnings("unchecked") // should only be null or set of strings
        Set<String> routingValues = (Set<String>) getPlannerResult(QueryPlanner.ROUTING_VALUES);
        if (routingValues != null && !routingValues.isEmpty()) {
            List<String> tmp = new ArrayList<>(routingValues.size());
            tmp.addAll(routingValues);
            request.routing(tmp.toArray(new String[tmp.size()]));
        }

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

    public SQLResponse buildResponse(CreateIndexResponse createIndexResponse) {
        return buildEmptyResponse(0);
    }

    public SQLResponse buildResponse(DeleteIndexResponse deleteIndexResponse) {
        return buildEmptyResponse(0);
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
        if (hasGroupBy()) {
            return false;
        }

        return countRequest;
    }

    public void setPlannerResult(String key, Object value) {
        plannerResults.put(key, value);
    }

    public Object getPlannerResult(String key) {
        return plannerResults.get(key);
    }

    public Map<String, Object> plannerResults() {
        return plannerResults;
    }

    public XContentVisitor visitor() {
        return visitor;
    }

    public boolean hasGroupBy() {
        return (groupByColumnNames != null && groupByColumnNames.size() > 0);
    }
}
