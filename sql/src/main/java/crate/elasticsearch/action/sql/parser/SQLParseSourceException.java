package crate.elasticsearch.action.sql.parser;

import org.elasticsearch.ElasticSearchException;

/**
 * An exception thrown if the XContent source of a request cannot be parsed.
 */
public class SQLParseSourceException extends ElasticSearchException {

    public SQLParseSourceException(SQLXContentSourceContext context, String msg) {
        super("Parse Failure [" + msg + "]");
    }

    public SQLParseSourceException(SQLXContentSourceContext context, String msg, Throwable cause) {
        super("Parse Failure [" + msg + "]", cause);
    }
}