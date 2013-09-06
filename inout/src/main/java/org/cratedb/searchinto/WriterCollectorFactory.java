package org.cratedb.searchinto;

import org.cratedb.action.searchinto.SearchIntoContext;

public interface WriterCollectorFactory {

    WriterCollector create(SearchIntoContext context);

}
