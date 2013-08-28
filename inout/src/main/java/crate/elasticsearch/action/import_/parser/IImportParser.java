package crate.elasticsearch.action.import_.parser;

import crate.elasticsearch.action.import_.ImportContext;
import org.elasticsearch.common.bytes.BytesReference;

/**
 *
 */
public interface IImportParser {
    public void parseSource(ImportContext context, BytesReference source) throws ImportParseException;
}
