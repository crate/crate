package crate.elasticsearch.searchinto;

import crate.elasticsearch.action.searchinto.SearchIntoContext;

public interface WriterCollectorFactory {

    WriterCollector create(SearchIntoContext context);

}
