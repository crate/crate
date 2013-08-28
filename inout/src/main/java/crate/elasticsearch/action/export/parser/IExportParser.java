package crate.elasticsearch.action.export.parser;

import crate.elasticsearch.action.export.ExportContext;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.search.SearchParseException;

/**
 * Interface for source parsers
 */
public interface IExportParser {

    public void parseSource(ExportContext context, BytesReference source) throws SearchParseException;
}
