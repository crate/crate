package org.cratedb.action.import_.parser;

import org.elasticsearch.ElasticSearchException;

import org.cratedb.action.import_.ImportContext;

public class ImportParseException extends ElasticSearchException {

    private static final long serialVersionUID = 910205724931139923L;

    public ImportParseException(ImportContext context, String msg) {
        super("Parse Failure [" + msg + "]");
    }

    public ImportParseException(ImportContext context, String msg, Throwable cause) {
        super("Parse Failure [" + msg + "]", cause);
    }
}
