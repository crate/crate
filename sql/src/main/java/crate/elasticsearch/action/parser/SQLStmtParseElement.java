package crate.elasticsearch.action.parser;

import com.akiban.sql.parser.SQLParser;
import crate.elasticsearch.sql.SQLParseException;
import org.elasticsearch.common.xcontent.XContentParser;

/**
 * used to parse the "stmt" element that is expected to be in requests parsed by the {@link SQLRequestParser}
 *
 * Fills the StatementNode in the {@link SQLContext}.
 */
public class SQLStmtParseElement implements SQLParseElement {

    private SQLParser sqlParser;

    public SQLStmtParseElement() {
        this.sqlParser = new SQLParser();
    }

    @Override
    public void parse(XContentParser parser, SQLContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();

        if (!token.isValue()) {
            throw new SQLParseException("Field [" + parser.currentName() + "] has an invalid value");
        }

        context.statementNode(sqlParser.parseStatement(parser.text()));
    }
}
