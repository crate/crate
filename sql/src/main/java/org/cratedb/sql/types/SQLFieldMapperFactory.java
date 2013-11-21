package org.cratedb.sql.types;

import org.cratedb.index.IndexMetaDataExtractor;

public interface SQLFieldMapperFactory {
    public SQLFieldMapper create(IndexMetaDataExtractor extractor);
}
