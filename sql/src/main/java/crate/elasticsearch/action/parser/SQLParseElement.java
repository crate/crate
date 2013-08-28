package crate.elasticsearch.action.parser;

import org.elasticsearch.common.xcontent.XContentParser;

/**
 * This interface is for the {@link SQLRequestParser}
 * The {@link SQLRequestParser} receives a JSON structured body in the form of:
 *
 * {
 *     "stmt": "...",
 *     "other_property": "..."
 * }
 *
 * it then utilizes a {@link SQLParseElement} for each property in that structure.
 *
 * E.g. for "stmt" property the {@link SQLStmtParseElement} is used.
 */
public interface SQLParseElement {

    void parse(XContentParser parser, SQLContext context) throws Exception;
}
