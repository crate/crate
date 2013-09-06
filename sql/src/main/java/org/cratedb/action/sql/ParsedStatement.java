package org.cratedb.action.sql;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.StatementNode;
import org.cratedb.action.parser.QueryVisitor;
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
    private final QueryVisitor visitor;

    public ParsedStatement(String stmt, NodeExecutionContext executionContext) throws
            StandardException {
        this.stmt = stmt;
        statementNode = parser.parseStatement(stmt);
        visitor = new QueryVisitor(executionContext);
        statementNode.accept(visitor);
        builder = visitor.getXContentBuilder();
        indices = visitor.getIndices();
        outputFields = visitor.outputFields();
        sqlFields = new SQLFields(outputFields);
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

    public String[] cols() {
        String[] cols = new String[outputFields.size()];
        for (int i = 0; i < outputFields.size(); i++) {
            cols[i] = outputFields.get(i).v1();
        }
        return cols;
    }

    public SQLResponse buildResponse(SearchResponse searchResponse) {

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
}
