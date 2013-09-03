package crate.elasticsearch.action.sql.parser;

/**
 * Context for information gathered by parsing an XContent based sql request
 */
public class SQLXContentSourceContext {

    private String stmt;

    public String stmt() {
        return stmt;
    }

    public void stmt(String stmt) {
        this.stmt = stmt;
    }
}
