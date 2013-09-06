package org.cratedb.action.sql.parser;

import org.elasticsearch.common.xcontent.XContentParser;

/**
 * This interface is for the {@link org.cratedb.action.sql.parser.SQLXContentSourceParser}
 * The {@link org.cratedb.action.sql.parser.SQLXContentSourceParser} receives a JSON structured body in the form of:
 *
 * {
 *     "stmt": "...",
 *     "other_property": "..."
 * }
 *
 * it then utilizes a {@link SQLParseElement} for each property in that structure.
 *
 * E.g. for "stmt" property the {@link org.cratedb.action.sql.parser.SQLStmtParseElement} is used.
 */
public interface SQLParseElement {

    void parse(XContentParser parser, SQLXContentSourceContext context) throws Exception;
}
