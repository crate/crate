package org.cratedb.action.sql;

import org.cratedb.sql.facet.InternalSQLFacet;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.NodeTypes;
import org.cratedb.sql.parser.parser.SQLParser;
import org.cratedb.sql.parser.parser.StatementNode;
import org.cratedb.action.parser.InsertVisitor;
import org.cratedb.action.parser.QueryVisitor;
import org.cratedb.action.parser.XContentVisitor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
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
            new ArrayList<Tuple<String, String>>();
    private final NodeExecutionContext context;
    private final List<String> indices = new ArrayList<String>(1);
    private ESLogger logger = Loggers.getLogger(ParsedStatement.class);

    private final String stmt;
    private final Object[] args;
    private final StatementNode statementNode;
    private final XContentVisitor visitor;

    public static final int SEARCH_ACTION = 1;
    public static final int INSERT_ACTION = 2;
    public static final int DELETE_ACTION = 3;
    public static final int BULK_ACTION = 4;
    private Map<String, Object> updateDoc;

    public ParsedStatement(String stmt, Object[] args, NodeExecutionContext context) throws
            StandardException {
        this.stmt = stmt;
        this.args = args;
        this.context = context;
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

    public int type() {
        switch (statementNode.getNodeType()) {
            case NodeTypes.INSERT_NODE:
                if (((InsertVisitor)visitor).isBulk()) {
                    return BULK_ACTION;
                }
                return INSERT_ACTION;
            case NodeTypes.DELETE_NODE:
                return DELETE_ACTION;
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

    public SQLResponse buildResponse(SearchResponse searchResponse) {

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
        SQLResponse response = new SQLResponse();
        response.cols(cols());
        response.rows(new Object[0][0]);

        return response;
    }

    public SQLResponse buildResponse(BulkResponse bulkResponse) {
        SQLResponse response = new SQLResponse();
        response.cols(cols());
        response.rows(new Object[0][0]);

        return response;
    }

    public SQLResponse buildResponse(DeleteByQueryResponse deleteByQueryResponse) {
        SQLResponse response = new SQLResponse();
        response.cols(cols());
        response.rows(new Object[0][0]);

        // TODO: add rows affected
        return response;
    }

    public DeleteByQueryRequest buildDeleteRequest() throws StandardException {
        DeleteByQueryRequest request = new DeleteByQueryRequest();
        builder = visitor.getXContentBuilder();
        request.query(builder.bytes().toBytes());
        request.indices(indices.toArray(new String[indices.size()]));

        return request;
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
}
