package crate.elasticsearch.searchinto;

import org.elasticsearch.search.SearchContextException;
import org.elasticsearch.search.internal.SearchContext;

public class WriterException extends SearchContextException {

    public WriterException(SearchContext context, String msg) {
        super(context, msg);
    }

    public WriterException(SearchContext context, String msg, Throwable t) {
        super(context, "Write Failed [" + msg + "]", t);
    }
}
