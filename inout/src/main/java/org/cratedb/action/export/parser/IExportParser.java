package org.cratedb.action.export.parser;

import org.cratedb.action.export.ExportContext;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.search.SearchParseException;

/**
 * Interface for source parsers
 */
public interface IExportParser {

    public void parseSource(ExportContext context, BytesReference source) throws SearchParseException;
}
