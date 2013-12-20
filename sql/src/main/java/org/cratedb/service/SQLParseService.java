package org.cratedb.service;

import org.cratedb.action.parser.context.HandlerContext;
import org.cratedb.action.parser.context.ParseContext;
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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.List;

public class SQLParseService {

    final ESLogger logger = Loggers.getLogger(getClass());

    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
    public static final Integer DEFAULT_SELECT_LIMIT = 10000;
    public final NodeExecutionContext nodeExecutionContext;

    @Inject
    public SQLParseService(NodeExecutionContext context) {
        this.nodeExecutionContext = context;
    }

    /**
     * parse an SQL statement in handler context
     */
    public ParsedStatement parse(String statement) throws SQLParseException {
        return parse(statement, EMPTY_OBJECT_ARRAY, HandlerContext.INSTANCE);
    }

    /**
     * parse the statement in handler context
     */
    public ParsedStatement parse(String statement, Object[] args) throws SQLParseException {
        return parse(statement, args, HandlerContext.INSTANCE);
    }

    /**
     * parse the statement on a node
     */
    public ParsedStatement parse(String statement, String nodeId) {
        return parse(statement, EMPTY_OBJECT_ARRAY, nodeId, null, null);
    }

    /**
     * parse the statement in a table context
     */
    public ParsedStatement parse(String statement, String nodeId, String tableName) {
        return parse(statement, EMPTY_OBJECT_ARRAY, nodeId, tableName, null);
    }

    /**
     * parse the statement on a node
     */
    public ParsedStatement parse(String statement, Object[] args, String nodeId) {
        return parse(statement, args, nodeId, null, null);
    }

    /**
     * parse the statement in a table context
     */
    public ParsedStatement parse(String statement, Object[] args, String nodeId, String tableName) {
        return parse(statement, args, nodeId, tableName, null);
    }

    /**
     * parse an SQL Statement without args
     * @param statement the statement String
     * @param nodeId the id of the node we parse on, if null, statement is parsed in cluster context
     * @param tableName the name of the table/index, if null, statement is parsed in node context
     * @param shardId the name of the table/index, if null, statement is parsed in table context,
     *                if not null, statement is parsed in shard context
     * @return the parsed statement
     * @throws SQLParseException if something went wrong
     */
    public ParsedStatement parse(String statement, @Nullable String nodeId, @Nullable String tableName, @Nullable Integer shardId) throws SQLParseException {
        return parse(statement, EMPTY_OBJECT_ARRAY, nodeId, tableName, shardId);
    }

    /**
     * parse an SQL Statement with args
     * @param statement the statement String
     * @param args the arguments to fill in {@link org.cratedb.sql.parser.parser.ParameterNode}s in the query
     * @param nodeId the id of the node we parse on, if null, statement is parsed in cluster context
     * @param tableName the name of the table/index, if null, statement is parsed in node context
     * @param shardId the name of the table/index, if null, statement is parsed in table context,
     *                if not null, statement is parsed in shard context
     * @return the parsed statement
     * @throws SQLParseException if something went wrong
     */
    public ParsedStatement parse(String statement, Object args[], @Nullable String nodeId, @Nullable String tableName, @Nullable Integer shardId) throws SQLParseException {
        return parse(statement, args, new ParseContext(nodeId, tableName, shardId));
    }

    private ParsedStatement parse(String statement, Object[] args, ParseContext parseContext) throws SQLParseException {
        StopWatch stopWatch = null;

        ParsedStatement stmt = new ParsedStatement(statement);

        if (logger.isTraceEnabled()) {
            stopWatch = new StopWatch().start();
        }
        try {
            SQLParser parser = new SQLParser();
            StatementNode statementNode = parser.parseStatement(statement);
            BaseVisitor visitor;
            switch (statementNode.getNodeType()) {
                case NodeTypes.INSERT_NODE:
                    visitor = new InsertVisitor(nodeExecutionContext, parseContext, stmt, args);
                    break;
                case NodeTypes.CREATE_TABLE_NODE:
                case NodeTypes.DROP_TABLE_NODE:
                    visitor = new TableVisitor(nodeExecutionContext, parseContext, stmt, args);
                    break;
                case NodeTypes.CREATE_ANALYZER_NODE:
                    visitor = new AnalyzerVisitor(nodeExecutionContext, parseContext, stmt, args);
                    break;
                case NodeTypes.COPY_STATEMENT_NODE:
                    visitor = new CopyVisitor(nodeExecutionContext, parseContext, stmt, args);
                    break;
                default:
                    visitor = new QueryVisitor(nodeExecutionContext, parseContext, stmt, args);
                    break;
            }
            statementNode.accept(visitor);
        } catch (CrateException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new SQLParseException(ex.getMessage(), ex);
        }

        if (logger.isTraceEnabled()) {
            assert stopWatch != null;
            stopWatch.stop();
            logger.trace("Parsing sql statement took {}", stopWatch.totalTime().getMillis());
        }
        return stmt;
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
