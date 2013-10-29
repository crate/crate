package org.cratedb.service;

import org.cratedb.action.parser.visitors.*;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.parser.NodeTypes;
import org.cratedb.sql.parser.parser.SQLParser;
import org.cratedb.sql.parser.parser.StatementNode;
import org.elasticsearch.common.inject.Inject;

public class SQLParseService {

    public static final Integer DEFAULT_SELECT_LIMIT = 1000;
    public final NodeExecutionContext context;

    @Inject
    public SQLParseService(NodeExecutionContext context) {
        this.context = context;
    }

    public ParsedStatement parse(String statement) throws SQLParseException {
        return parse(statement, new Object[0]);
    }

    public ParsedStatement parse(String statement, Object[] args) throws SQLParseException {
        try {
            SQLParser parser = new SQLParser();
            ParsedStatement stmt = new ParsedStatement(statement);
            StatementNode statementNode = parser.parseStatement(statement);
            BaseVisitor visitor;
            switch (statementNode.getNodeType()) {
                case NodeTypes.INSERT_NODE:
                    visitor = new InsertVisitor(context, stmt, args);
                    break;
                case NodeTypes.CREATE_TABLE_NODE:
                case NodeTypes.DROP_TABLE_NODE:
                    visitor = new TableVisitor(context, stmt, args);
                    break;
                case NodeTypes.CREATE_ANALYZER_NODE:
                    visitor = new AnalyzerVisitor(context, stmt, args);
                    break;
                default:
                    visitor = new QueryVisitor(context, stmt, args);
                    break;
            }
            statementNode.accept(visitor);
            return stmt;
        } catch (CrateException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new SQLParseException(ex.getMessage(), ex);
        }
    }
}
