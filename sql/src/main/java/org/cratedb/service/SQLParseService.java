package org.cratedb.service;

import org.cratedb.action.parser.visitors.*;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.NodeTypes;
import org.cratedb.sql.parser.parser.ParameterNode;
import org.cratedb.sql.parser.parser.SQLParser;
import org.cratedb.sql.parser.parser.StatementNode;
import org.cratedb.sql.parser.unparser.NodeToString;
import org.elasticsearch.common.inject.Inject;

import java.util.List;

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

    /**
     * unparse a parsed statement to an SQL String
     * if args are given replace Parameters with them
     * @param stmt the ParsedStatement to unparse
     * @param args if given tries to replace the parameters in the statement with these values
     * @return the SQL Statement as String
     * @throws StandardException if unparsing failed or the number of arguments is too small
     */
    public String unparse(ParsedStatement stmt, Object[] args) throws StandardException {
        return unparse(stmt.stmt, args);
    }

    public String unparse(ParsedStatement stmt) throws StandardException {
        return unparse(stmt, null);
    }

    public String unparse(String stmt, Object[] args) throws StandardException {
        SQLParser parser = new SQLParser();
        StatementNode node = parser.parseStatement(stmt);
        String result = new NodeToString().toString(node);

        if (args != null && args.length > 0) {
            List<ParameterNode> parameters = parser.getParameterList();
            for (ParameterNode parameterNode : parameters) {
                int paramNumber = parameterNode.getParameterNumber();
                if (paramNumber >= args.length) {
                    throw new StandardException("not enough arguments");
                }
                Object replacement = args[paramNumber];
                if (replacement instanceof String) {
                    replacement = "'" + replacement + "'";
                }
                result = result.replace(
                        String.format("$%d", paramNumber+1),
                        replacement.toString());
            }
        }

        return result;
    }
}
