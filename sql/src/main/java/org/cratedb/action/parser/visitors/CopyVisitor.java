package org.cratedb.action.parser.visitors;

import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.parser.parser.CopyStatementNode;

public class CopyVisitor extends BaseVisitor {

    public CopyVisitor(NodeExecutionContext context, ParsedStatement parsedStatement, Object[] args) {
        super(context, parsedStatement, args);
    }

    @Override
    public void visit(CopyStatementNode node) throws Exception {
        tableName(node.getTableName());

        stmt.importPath = (String)valueFromNode(node.getFilename());

        stmt.type(ParsedStatement.ActionType.COPY_IMPORT_ACTION);
    }
}
