package org.cratedb.action.sql;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.NodeTypes;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.StatementNode;
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
import java.util.List;

public class ParsedStatement {

    private final SQLFields sqlFields;
    private final XContentBuilder builder;
    private final List<Tuple<String, String>> outputFields;
    private List<String> indices;
    private ESLogger logger = Loggers.getLogger(ParsedStatement.class);

    private final String stmt;
    private final SQLParser parser = new SQLParser();
    private final StatementNode statementNode;
    private final XContentVisitor visitor;

    public static final int SEARCH_ACTION = 1;
    public static final int INSERT_ACTION = 2;
    public static final int DELETE_ACTION = 3;
    public static final int BULK_ACTION = 4;

    public ParsedStatement(String stmt, Object[] args, NodeExecutionContext executionContext) throws
            StandardException {
        this.stmt = stmt;
        statementNode = parser.parseStatement(stmt);
        switch (statementNode.getNodeType()) {
            case NodeTypes.INSERT_NODE:
                visitor = new InsertVisitor(executionContext, args);
                break;
            default:
                visitor = new QueryVisitor(executionContext, args);
                break;
        }
        statementNode.accept(visitor);
        builder = visitor.getXContentBuilder();
        indices = visitor.getIndices();
        outputFields = visitor.outputFields();
        sqlFields = new SQLFields(outputFields);
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
        IndexRequest request = new IndexRequest();
        if (logger.isDebugEnabled()) {
            builder.generator().usePrettyPrint();
            try {
                logger.info("converted sql to: " + builder.string());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // We only support 1 ES type per index, it's named: ``default``
        request.type("default");

        request.index(indices.get(0));
        request.source(builder.bytes().toBytes());
        return request;
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

        String defaultIndex = "index";
        String defaultType = "default";

        request.add(builder.bytes(), false, defaultIndex, defaultType);

        return request;
    }

    public String[] cols() {
        String[] cols = new String[outputFields.size()];
        for (int i = 0; i < outputFields.size(); i++) {
            cols[i] = outputFields.get(i).v1();
        }
        return cols;
    }

    public SQLResponse buildResponse(SearchResponse searchResponse) {

        SearchHit[] searchHits = searchResponse.getHits().getHits();
        Object[][] rows = new Object[searchHits.length][outputFields.size()];

        for (int i = 0; i < searchHits.length; i++) {
            SearchHit hit = searchHits[i];
            sqlFields.hit(hit);
            rows[i] = sqlFields.getRowValues();
        }

        SQLResponse response = new SQLResponse();
        response.cols(cols());
        response.rows(rows);
        return response;
    }

    public SQLResponse buildResponse(IndexResponse indexResponse) {
        SQLResponse response = new SQLResponse();
        response.cols(cols());
        response.rows(((InsertVisitor)visitor).rows());

        return response;
    }

    public SQLResponse buildResponse(BulkResponse bulkResponse) {
        SQLResponse response = new SQLResponse();
        response.cols(cols());
        response.rows(((InsertVisitor)visitor).rows());

        return response;
    }

    public SQLResponse buildResponse(DeleteByQueryResponse deleteByQueryResponse) {
        SQLResponse response = new SQLResponse();
        response.cols(cols());
        response.rows(new Object[0][0]);

        // TODO: add rows affected
        return response;
    }

    public DeleteByQueryRequest buildDeleteRequest() {
        DeleteByQueryRequest request = new DeleteByQueryRequest();
        request.query(builder.bytes().toBytes());
        request.indices(indices.toArray(new String[indices.size()]));

        return request;
    }
}
