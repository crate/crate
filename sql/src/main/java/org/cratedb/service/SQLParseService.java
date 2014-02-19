/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.cratedb.service;

import com.google.common.collect.ImmutableList;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.Statement;
import io.crate.sql.tree.Table;
import org.cratedb.action.parser.visitors.*;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.NodeType;
import org.cratedb.sql.parser.parser.ParameterNode;
import org.cratedb.sql.parser.parser.SQLParser;
import org.cratedb.sql.parser.parser.StatementNode;
import org.cratedb.sql.parser.unparser.NodeToString;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.List;

public class SQLParseService {

    final ESLogger logger = Loggers.getLogger(getClass());

    public static final Integer DEFAULT_SELECT_LIMIT = 10000;
    public final NodeExecutionContext context;

    @Inject
    public SQLParseService(NodeExecutionContext context) {
        this.context = context;
    }

    public ParsedStatement parse(String statement) throws SQLParseException {
        return parse(statement, new Object[0]);
    }

    public ParsedStatement parse(String statement, Object[] args) throws SQLParseException {
        StopWatch stopWatch = null;
        ParsedStatement stmt;

        if (logger.isTraceEnabled()) {
            stopWatch = new StopWatch().start();
        }
        try {
            SQLParser parser = new SQLParser();
            StatementNode statementNode = parser.parseStatement(statement);
            stmt = parse(statement, statementNode, args);
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

    public ParsedStatement parse(String sql, StatementNode statementNode, Object[] args) {
        ParsedStatement stmt = new ParsedStatement(sql);
        BaseVisitor visitor;

        try {
            switch (statementNode.getNodeType()) {
                case INSERT_NODE:
                    visitor = new InsertVisitor(context, stmt, args);
                    break;
                case CREATE_TABLE_NODE:
                case DROP_TABLE_NODE:
                    visitor = new TableVisitor(context, stmt, args);
                    break;
                case CREATE_ANALYZER_NODE:
                    visitor = new AnalyzerVisitor(context, stmt, args);
                    break;
                case COPY_STATEMENT_NODE:
                    visitor = new CopyVisitor(context, stmt, args);
                    break;
                default:
                    visitor = new QueryVisitor(context, stmt, args);
                    break;
            }

            statementNode.accept(visitor);

        } catch (CrateException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new SQLParseException(ex.getMessage(), ex);
        }

        return stmt;
    }
}
