package crate.elasticsearch.action;

import com.akiban.sql.StandardException;
import crate.elasticsearch.action.parser.QueryVisitor;
import crate.elasticsearch.action.parser.SQLContext;
import crate.elasticsearch.action.parser.SQLRequestParser;
import crate.elasticsearch.action.parser.XContentGenerator;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used to generate ES Request classes (currently only {@link SearchRequest} from SQL Requests
 *
 * An SQL Request is in the form of:
 *
 * {
 *     "stmt": "...:"
 * }
 *
 * E.g. the searchRequest will then contain a query in the form of
 *
 * {
 *     "query": {
 *         "fields": [...],
 *         "term": {...}
 *     }
 * }
 */
public class SQLRequestBuilder {

    private SQLRequestParser parser;
    private SQLContext context;
    private ESLogger logger = Loggers.getLogger(SQLRequestBuilder.class);
    private Map<String, String> fieldNameMapping;

    public SQLRequestBuilder() {
        context = new SQLContext();
        fieldNameMapping = null;
    }

    public SQLRequestBuilder source(String source) {
        return this.source(new BytesArray(source));
    }

    /**
     * Parses the source and fills the internally used {@link SQLContext}
     *
     * After setting the source other methods like {@link #buildSearchRequest()} can be used.
     * @param source
     * @return
     */
    public SQLRequestBuilder source(BytesReference source) {
        parser = new SQLRequestParser();
        parser.parseSource(context, source);
        return this;
    }

    /**
     * This method converts the SQL-Syntax tree into XContent-Format and then returns a {@link SearchRequest}
     * {@link #source(org.elasticsearch.common.bytes.BytesReference)}
     * or {@link #source(String)} has to be called before the SearchRequest can be built.
     *
     * @return built SearchRequest that has source() and indices() set from the parsed SQL.
     * @throws StandardException in case there was an error parsing the SQL Statements or traversing the SQL-Syntax Tree
     */
    public SearchRequest buildSearchRequest() throws StandardException {
        SearchRequest request = new SearchRequest();

        QueryVisitor visitor = new QueryVisitor();
        context.statementNode().accept(visitor);
        XContentBuilder builder = visitor.getXContentBuilder();

        logger.debug("converted sql to: " + builder.bytes().toUtf8());
        request.source(builder.bytes().toBytes());
        request.indices(visitor.getIndices().toArray(new String[visitor.getIndices().size()]));
        fieldNameMapping = visitor.getFieldnameMapping();

        return request;
    }

    /**
     * See {@link crate.elasticsearch.action.parser.XContentGenerator#getFieldNameMapping()}
     * @return
     */
    public Map<String, String> getFieldNameMapping() {
        return fieldNameMapping;
    }
}
