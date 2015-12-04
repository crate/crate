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

import io.crate.sql.ExpressionFormatter;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.*;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class ShowStatementAnalyzer {

    private final SelectStatementAnalyzer selectStatementAnalyzer;

    private String[] explicitSchemas = new String[]{"information_schema", "sys"};

    @Inject
    public ShowStatementAnalyzer(SelectStatementAnalyzer selectStatementAnalyzer) {
        this.selectStatementAnalyzer = selectStatementAnalyzer;
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
        StringBuilder sb = new StringBuilder("SELECT schema_name ");
        sb.append("FROM information_schema.schemata ");
        if (node.likePattern().isPresent()) {
            sb.append(String.format("WHERE schema_name LIKE '%s' ", node.likePattern().get()));
        } else if (node.whereExpression().isPresent()) {
            sb.append(String.format("WHERE %s ", node.whereExpression().get().toString()));
        }
        sb.append("ORDER BY schema_name");
        Statement statement = SqlParser.createStatement(sb.toString());
        return selectStatementAnalyzer.visitQuery((Query) statement, analysis);
    }

    public AnalyzedStatement analyze(ShowColumns node, Analysis analysis) {
        /**
         * <code>
         *     SHOW COLUMNS {FROM | IN} table [{FROM | IN} schema] [LIKE 'pattern' | WHERE expr];
         * </code>
         * needs to be rewritten to select query:
         * <code>
         * SELECT column_name, data_type, column_name = ANY (constraint_name) as primary_key
         * FROM information_schema.columns cl INNER JOIN information_schema.table_constraints cn
         * ON cl.table_name = cn.table_name AND cl.schema_name = cn.schema_name AND
         * cl.schema_name = {'doc' | ['%s'] }
         * [ AND WHERE cl.table_name LIKE '%s' | column_name LIKE '%s']
         * ORDER BY column_name;
         * </code>
         */
        StringBuilder sb =
                new StringBuilder(
                        "SELECT column_name, data_type, column_name = ANY (constraint_name) as primary_key " +
                        "FROM information_schema.columns cl INNER JOIN information_schema.table_constraints cn " +
                        "ON cl.table_name = cn.table_name AND cl.schema_name = cn.schema_name ");
        sb.append(String.format("WHERE cl.table_name = '%s' ", node.table().toString()));
        if (node.schema().isPresent()) {
            sb.append(String.format("AND cl.schema_name = '%s' ", node.schema().get()));
        } else {
            sb.append("AND cl.schema_name = 'doc' ");
        }
        if (node.likePattern().isPresent()) {
            sb.append(String.format("AND column_name LIKE '%s' ", node.likePattern().get()));
        } else if (node.where().isPresent()) {
            sb.append(String.format("AND %s", ExpressionFormatter.formatExpression(node.where().get())));
        }
        sb.append("ORDER BY column_name");

        Statement statement = SqlParser.createStatement(sb.toString());
        return selectStatementAnalyzer.visitQuery((Query) statement, analysis);
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
        StringBuilder sb = new StringBuilder("SELECT distinct(table_name) as table_name FROM information_schema.tables ");

        if (node.schema().isPresent()) {
            sb.append(String.format("WHERE schema_name = '%s'", node.schema().get()));
        } else {
            sb.append("WHERE schema_name NOT IN (");
            for (int i = 0; i < explicitSchemas.length; i++) {
                sb.append(String.format("'%s'", explicitSchemas[i]));
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
            sb.append(String.format(" AND table_name like '%s'", node.likePattern().get()));
        }

        sb.append(" ORDER BY 1");
        Statement statement = SqlParser.createStatement(sb.toString());
        return selectStatementAnalyzer.visitQuery((Query) statement, analysis);
    }
}
