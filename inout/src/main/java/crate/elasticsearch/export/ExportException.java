package crate.elasticsearch.export;

import org.elasticsearch.search.SearchContextException;
import org.elasticsearch.search.internal.SearchContext;

/**
 * Created with IntelliJ IDEA.
 * User: bd
 * Date: 9.4.13
 * Time: 14:19
 * To change this template use File | Settings | File Templates.
 */
public class ExportException extends SearchContextException {

    public ExportException(SearchContext context, String msg) {
        super(context, msg);
    }

    public ExportException(SearchContext context, String msg, Throwable t) {
        super(context, "Export Failed [" + msg + "]", t);
    }
}
