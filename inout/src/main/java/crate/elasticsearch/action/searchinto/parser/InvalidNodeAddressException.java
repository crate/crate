package crate.elasticsearch.action.searchinto.parser;

import org.elasticsearch.search.SearchContextException;
import org.elasticsearch.search.internal.SearchContext;

/**
 * An exception indicating a given node address used for TransportClient was invalid
 */
public class InvalidNodeAddressException extends SearchContextException {

    public InvalidNodeAddressException(SearchContext context, String msg) {
        super(context, "Invalid Address [" + msg + "]");
    }

    public InvalidNodeAddressException(SearchContext context, String msg, Throwable t) {
        super(context, "Invalid Address [" + msg + "]", t);
    }
}
