/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import io.crate.action.sql.SessionContext;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.metadata.Schemas;
import io.crate.sql.ExpressionFormatter;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.*;
import org.elasticsearch.common.collect.Tuple;

import java.util.*;

class ShowStatementAnalyzer {

    private Analyzer analyzer;

    private String[] explicitSchemas = new String[]{"information_schema", "sys", "pg_catalog"};

    ShowStatementAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    Query rewriteShowTransaction() {
        /*
         * Rewrite
         *     SHOW TRANSACTION ISOLATION LEVEL
         * To
         *      select 'read uncommitted' from sys.cluster
         *
         * In order to return a single row with 'read uncommitted`.
         *
         * 'read uncommitted' is used because it matches the behaviour in Crate best.
         * Although there would be read-isolation on a lucene-reader basis there are no transactions so anything written
         * by any other client *may* become visible at any time.
         *
         * See https://www.postgresql.org/docs/9.5/static/transaction-iso.html
         */
        return (Query) SqlParser.createStatement("select 'read uncommitted' as transaction_isolation from sys.cluster");
    }

    AnalyzedStatement analyzeShowTransaction(Analysis analysis) {
        Query query = rewriteShowTransaction();
        Analysis newAnalysis = analyzer.boundAnalyze(query, analysis.sessionContext(), ParameterContext.EMPTY);
        analysis.rootRelation(newAnalysis.rootRelation());
        return newAnalysis.analyzedStatement();
    }

    Tuple<Query, ParameterContext> rewriteShow(ShowSchemas node) {
        /*
         * <code>
         *     SHOW SCHEMAS [ LIKE 'pattern' ];
         * </code>
         * needs to be rewritten to select query:
         * <code>
         *     SELECT schema_name
         *     FROM information_schema.schemata
         *     [ WHERE schema_name LIKE 'pattern' ]
         *     ORDER BY schema_name;
         * </code>
         */
        List<String> params = new ArrayList<>();
        StringBuilder sb = new StringBuilder("SELECT schema_name ");
        sb.append("FROM information_schema.schemata ");
        if (node.likePattern().isPresent()) {
            params.add(node.likePattern().get());
            sb.append("WHERE schema_name LIKE ? ");
        } else if (node.whereExpression().isPresent()) {
            sb.append(String.format(Locale.ENGLISH, "WHERE %s ", node.whereExpression().get().toString()));
        }
        sb.append("ORDER BY schema_name");
        return new Tuple<>(
            (Query) SqlParser.createStatement(sb.toString()),
            new ParameterContext(new RowN(params.toArray(new Object[0])), Collections.<Row>emptyList())
        );
    }

    public AnalyzedStatement analyze(ShowSchemas node, Analysis analysis) {
        Tuple<Query, ParameterContext> tuple = rewriteShow(node);
        Analysis newAnalysis = analyzer.boundAnalyze(tuple.v1(), analysis.sessionContext(), tuple.v2());
        analysis.rootRelation(newAnalysis.rootRelation());
        return newAnalysis.analyzedStatement();
    }


    Tuple<Query, ParameterContext> rewriteShow(ShowColumns node, String defaultSchema) {
        /*
         * <code>
         *     SHOW COLUMNS {FROM | IN} table [{FROM | IN} schema] [LIKE 'pattern' | WHERE expr];
         * </code>
         * needs to be rewritten to select query:
         * <code>
         * SELECT column_name, data_type
         * FROM information_schema.columns
         * WHERE table_schema = {'doc' | ['%s'] }
         * [ AND WHERE table_name LIKE '%s' | column_name LIKE '%s']
         * ORDER BY column_name;
         * </code>
         */
        List<String> params = new ArrayList<>();
        StringBuilder sb =
            new StringBuilder(
                "SELECT column_name, data_type " +
                "FROM information_schema.columns ");
        params.add(node.table().toString());
        sb.append("WHERE table_name = ? ");

        sb.append("AND table_schema = ? ");
        if (node.schema().isPresent()) {
            params.add(node.schema().get().toString());
        } else {
            params.add(defaultSchema);
        }
        if (node.likePattern().isPresent()) {
            params.add(node.likePattern().get());
            sb.append("AND column_name LIKE ? ");
        } else if (node.where().isPresent()) {
            sb.append(String.format(Locale.ENGLISH, "AND %s", ExpressionFormatter.formatExpression(node.where().get())));
        }
        sb.append("ORDER BY column_name");
        return new Tuple<>(
            (Query) SqlParser.createStatement(sb.toString()),
            new ParameterContext(new RowN(params.toArray(new Object[0])), Collections.<Row>emptyList())
        );
    }

    public AnalyzedStatement analyze(ShowColumns node, Analysis analysis) {
        SessionContext sessionContext = analysis.sessionContext();
        Tuple<Query, ParameterContext> tuple = rewriteShow(node, sessionContext.defaultSchema());
        Analysis newAnalysis = analyzer.boundAnalyze(tuple.v1(), sessionContext, tuple.v2());
        analysis.rootRelation(newAnalysis.rootRelation());
        return newAnalysis.analyzedStatement();
    }

    public AnalyzedStatement analyze(ShowTables node, Analysis analysis) {
        Tuple<Query, ParameterContext> tuple = rewriteShow(node);
        Analysis newAnalysis = analyzer.boundAnalyze(tuple.v1(), analysis.sessionContext(), tuple.v2());
        analysis.rootRelation(newAnalysis.rootRelation());
        return newAnalysis.analyzedStatement();
    }

    public Tuple<Query, ParameterContext> rewriteShow(ShowTables node) {
        /*
         * <code>
         *     SHOW TABLES [{ FROM | IN } table_schema ] [ { LIKE 'pattern' | WHERE expr } ];
         * </code>
         * needs to be rewritten to select query:
         * <code>
         *     SELECT distinct(table_name) as table_name
         *     FROM information_schema.tables
         *     [ WHERE ( table_schema = 'table_schema' | table_schema NOT IN ( 'sys', 'information_schema' ) )
         *      [ AND ( table_name LIKE 'pattern' | <where-clause > ) ]
         *     ]
         *     ORDER BY 1;
         * </code>
         */
        List<String> params = new ArrayList<>();
        StringBuilder sb = new StringBuilder("SELECT distinct(table_name) as table_name FROM information_schema.tables ");
        Optional<QualifiedName> schema = node.schema();
        if (schema.isPresent()) {
            params.add(schema.get().toString());
            sb.append("WHERE table_schema = ?");
        } else {
            sb.append("WHERE table_schema NOT IN (");
            for (int i = 0; i < explicitSchemas.length; i++) {
                params.add(explicitSchemas[i]);
                sb.append("?");
                if (i < explicitSchemas.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append(")");
        }
        Optional<Expression> whereExpression = node.whereExpression();
        if (whereExpression.isPresent()) {
            sb.append(" AND (");
            sb.append(whereExpression.get().toString());
            sb.append(")");
        } else {
            Optional<String> likePattern = node.likePattern();
            if (likePattern.isPresent()) {
                params.add(likePattern.get());
                sb.append(" AND table_name like ?");
            }
        }
        sb.append(" ORDER BY 1");

        return new Tuple<>(
            (Query) SqlParser.createStatement(sb.toString()),
            new ParameterContext(new RowN(params.toArray(new Object[0])), Collections.<Row>emptyList())
        );
    }
}
