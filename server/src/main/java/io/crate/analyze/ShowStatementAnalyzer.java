/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import java.util.Locale;
import java.util.Optional;

import org.jetbrains.annotations.Nullable;

import io.crate.metadata.RelationInfo;
import io.crate.metadata.Schemas;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.ExpressionFormatter;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.ShowColumns;
import io.crate.sql.tree.ShowSchemas;
import io.crate.sql.tree.ShowSessionParameter;
import io.crate.sql.tree.ShowTables;
import io.crate.sql.tree.Table;

/**
 * Rewrites the SHOW statements into Select queries.
 *
 * Note: Parameters passed by the user _must_ remain the same to be compatible with
 * the Postgres ParameterDescription message. Therefore, the rewritten query must
 * make use of the passed parameter and not add any additional parameters.
 */
class ShowStatementAnalyzer {

    private final Analyzer analyzer;

    private final String[] explicitSchemas = new String[]{"information_schema", "sys", "pg_catalog"};

    private final Schemas schemas;

    private final SessionSettingRegistry sessionSettingRegistry;

    ShowStatementAnalyzer(Analyzer analyzer, Schemas schemas, SessionSettingRegistry sessionSettingRegistry) {
        this.analyzer = analyzer;
        this.schemas = schemas;
        this.sessionSettingRegistry = sessionSettingRegistry;
    }

    Query rewriteShowTransaction() {
        /*
         * Rewrite
         *     SHOW TRANSACTION ISOLATION LEVEL
         * To
         *      select 'read uncommitted'
         *
         * In order to return a single row with 'read uncommitted`.
         *
         * 'read uncommitted' is used because it matches the behaviour in Crate best.
         * Although there would be read-isolation on a lucene-reader basis there are no transactions so anything written
         * by any other client *may* become visible at any time.
         *
         * See https://www.postgresql.org/docs/9.5/static/transaction-iso.html
         */
        return (Query) SqlParser.createStatement("select 'read uncommitted' as transaction_isolation");
    }

    AnalyzedStatement analyzeShowTransaction(Analysis analysis) {
        return unboundAnalyze(rewriteShowTransaction(), analysis);
    }

    public AnalyzedStatement analyzeShowCreateTable(Table<?> table, Analysis analysis) {
        CoordinatorSessionSettings sessionSettings = analysis.sessionSettings();
        RelationInfo relation = schemas.resolveRelationInfo(
            table.getName(),
            Operation.SHOW_CREATE,
            sessionSettings.sessionUser(),
            sessionSettings.searchPath()
        );
        if (!(relation instanceof TableInfo tableInfo)) {
            throw new UnsupportedOperationException(
                "Cannot use SHOW CREATE TABLE on relation " + relation.ident() + " of type " + relation.relationType());
        }
        return new AnalyzedShowCreateTable(tableInfo);
    }

    Query rewriteShowSessionParameter(ShowSessionParameter node) {
        /*
         * Rewrite
         * <code>
         *     SHOW { parameter_name | ALL }
         * </code>
         * To
         * <code>
         *     SELECT [ name, ] setting
         *     FROM pg_catalog.pg_settings
         *     [ WHERE name = parameter_name ]
         * </code>
         */
        StringBuilder sb = new StringBuilder("SELECT ");
        QualifiedName sessionSetting = node.parameter();
        if (sessionSetting != null) {
            sb.append("setting ");
        } else {
            sb.append("name, setting, short_desc as description ");
        }
        sb.append("FROM pg_catalog.pg_settings ");
        if (sessionSetting != null) {
            sb.append("WHERE name = ");
            singleQuote(sb, sessionSetting.toString());
        }
        return (Query) SqlParser.createStatement(sb.toString());
    }

    void validateSessionSetting(@Nullable QualifiedName settingParameter) {
        if (settingParameter != null &&
            !sessionSettingRegistry.settings().containsKey(settingParameter.toString())) {
            throw new IllegalArgumentException(
                "Unknown session setting name '" + settingParameter + "'.");
        }
    }

    Query rewriteShowSchemas(ShowSchemas node) {
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
        StringBuilder sb = new StringBuilder("SELECT schema_name ");
        sb.append("FROM information_schema.schemata ");
        String likePattern = node.likePattern();
        if (likePattern != null) {
            sb.append("WHERE schema_name LIKE ");
            singleQuote(sb, likePattern);
        } else if (node.whereExpression().isPresent()) {
            sb.append(String.format(Locale.ENGLISH, "WHERE %s ", node.whereExpression().get().toString()));
        }
        sb.append("ORDER BY schema_name");
        return (Query) SqlParser.createStatement(sb.toString());
    }

    Query rewriteShowColumns(ShowColumns node, String defaultSchema) {
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
        StringBuilder sb =
            new StringBuilder(
                "SELECT column_name, data_type " +
                "FROM information_schema.columns ");
        sb.append("WHERE table_name = ");
        singleQuote(sb, node.table().toString());

        sb.append("AND table_schema = ");
        QualifiedName schema = node.schema();
        if (schema != null) {
            singleQuote(sb, schema.toString());
        } else {
            singleQuote(sb, defaultSchema);
        }
        String likePattern = node.likePattern();
        if (likePattern != null) {
            sb.append("AND column_name LIKE ");
            singleQuote(sb, likePattern);
        } else if (node.where().isPresent()) {
            sb.append(String.format(Locale.ENGLISH, "AND %s", ExpressionFormatter.formatExpression(node.where().get())));
        }
        sb.append("ORDER BY column_name");
        return (Query) SqlParser.createStatement(sb.toString());
    }

    private AnalyzedStatement unboundAnalyze(Query query, Analysis analysis) {
        return analyzer.analyze(
            query,
            analysis.sessionSettings(),
            analysis.paramTypeHints(),
            analysis.cursors()
        );
    }

    public Query rewriteShowTables(ShowTables node) {
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
        StringBuilder sb = new StringBuilder(
            "SELECT distinct(table_name) as table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' ");
        QualifiedName schema = node.schema();
        if (schema != null) {
            sb.append("AND table_schema = ");
            singleQuote(sb, schema.toString());
        } else {
            sb.append("AND table_schema NOT IN (");
            for (int i = 0; i < explicitSchemas.length; i++) {
                singleQuote(sb, explicitSchemas[i]);
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
            String likePattern = node.likePattern();
            if (likePattern != null) {
                sb.append(" AND table_name like ");
                singleQuote(sb, likePattern);
            }
        }
        sb.append(" ORDER BY 1");

        return (Query) SqlParser.createStatement(sb.toString());
    }

    private static void singleQuote(StringBuilder builder, String string) {
        builder.append("'").append(string).append("'");
    }
}
