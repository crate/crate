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

import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.metadata.Schemas;
import io.crate.sql.ExpressionFormatter;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.ShowColumns;
import io.crate.sql.tree.ShowSchemas;
import io.crate.sql.tree.ShowTables;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

@Singleton
public class ShowStatementAnalyzer {

    private Analyzer analyzer;

    private String[] explicitSchemas = new String[]{"information_schema", "sys", "pg_catalog"};

    public ShowStatementAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public AnalyzedStatement analyze(ShowSchemas node, Analysis analysis) {
        /**
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

        Analysis newAnalysis = analyzer.analyze(SqlParser.createStatement(sb.toString()),
                new ParameterContext(new RowN(params.toArray(new Object[0])), Collections.<Row>emptyList(), null));
        analysis.rootRelation(newAnalysis.rootRelation());
        return newAnalysis.analyzedStatement();
    }

    public AnalyzedStatement analyze(ShowColumns node, Analysis analysis) {
        /**
         * <code>
         *     SHOW COLUMNS {FROM | IN} table [{FROM | IN} schema] [LIKE 'pattern' | WHERE expr];
         * </code>
         * needs to be rewritten to select query:
         * <code>
         * SELECT column_name, data_type
         * FROM information_schema.columns
         * WHERE schema_name = {'doc' | ['%s'] }
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

        sb.append("AND schema_name = ? ");
        if (node.schema().isPresent()) {
            params.add(node.schema().get().toString());
        } else {
            params.add(Schemas.DEFAULT_SCHEMA_NAME);
        }
        if (node.likePattern().isPresent()) {
            params.add(node.likePattern().get());
            sb.append("AND column_name LIKE ? ");
        } else if (node.where().isPresent()) {
            sb.append(String.format(Locale.ENGLISH, "AND %s", ExpressionFormatter.formatExpression(node.where().get())));
        }
        sb.append("ORDER BY column_name");

        Analysis newAnalysis = analyzer.analyze(SqlParser.createStatement(sb.toString()),
                new ParameterContext(new RowN(params.toArray()), Collections.<Row>emptyList(), null));
        analysis.rootRelation(newAnalysis.rootRelation());
        return newAnalysis.analyzedStatement();
    }

    public AnalyzedStatement analyze(ShowTables node, Analysis analysis) {
        /**
         * <code>
         *     SHOW TABLES [{ FROM | IN } schema_name ] [ { LIKE 'pattern' | WHERE expr } ];
         * </code>
         * needs to be rewritten to select query:
         * <code>
         *     SELECT distinct(table_name) as table_name
         *     FROM information_schema.tables
         *     [ WHERE ( schema_name = 'schema_name' | schema_name NOT IN ( 'sys', 'information_schema' ) )
         *      [ AND ( table_name LIKE 'pattern' | <where-clause > ) ]
         *     ]
         *     ORDER BY 1;
         * </code>
         */
        List<String> params = new ArrayList<>();
        StringBuilder sb = new StringBuilder("SELECT distinct(table_name) as table_name FROM information_schema.tables ");
        if (node.schema().isPresent()) {
            params.add(node.schema().get().toString());
            sb.append("WHERE schema_name = ?");
        } else {
            sb.append("WHERE schema_name NOT IN (");
            for (int i = 0; i < explicitSchemas.length; i++) {
                params.add(explicitSchemas[i]);
                sb.append("?");
                if (i < explicitSchemas.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append(")");
        }
        if (node.whereExpression().isPresent()) {
            sb.append(" AND (");
            sb.append(node.whereExpression().get().toString());
            sb.append(")");
        } else if (node.likePattern().isPresent()) {
            params.add(node.likePattern().get());
            sb.append(" AND table_name like ?");
        }
        sb.append(" ORDER BY 1");

        Analysis newAnalysis = analyzer.analyze(SqlParser.createStatement(sb.toString()),
                new ParameterContext(new RowN(params.toArray()), Collections.<Row>emptyList(), null));
        analysis.rootRelation(newAnalysis.rootRelation());
        return newAnalysis.analyzedStatement();
    }
}
