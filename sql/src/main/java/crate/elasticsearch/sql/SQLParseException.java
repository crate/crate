package crate.elasticsearch.sql;

public class SQLParseException extends RuntimeException {

    public SQLParseException(String msg) {
        super(msg);
    }

    public SQLParseException(String msg, Exception e) {
        super(msg, e);
    }
}
