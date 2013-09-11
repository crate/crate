package org.cratedb.action.sql.parser;

/**
 * Context for information gathered by parsing an XContent based sql request
 */
public class SQLXContentSourceContext {

    private String stmt;
    private Object[] args;

    public String stmt() {
        return stmt;
    }

    public void stmt(String stmt) {
        this.stmt = stmt;
    }

    public Object[] args() {
        return args;
    }

    public void args(Object[] args) {
        this.args = args;
    }
}
