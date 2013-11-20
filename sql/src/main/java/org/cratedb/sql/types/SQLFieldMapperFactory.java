package org.cratedb.sql.types;

import org.cratedb.core.IndexMetaDataExtractor;

public interface SQLFieldMapperFactory {
    public SQLFieldMapper create(IndexMetaDataExtractor extractor);
}
