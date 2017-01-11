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

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.isSQL;

public class ShowStatementsAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor executor;

    @Before
    public void prepare() {
        executor = SQLExecutor.builder(clusterService).build();
    }

    private SelectAnalyzedStatement analyze(String stmt) {
        return executor.analyze(stmt);
    }

    @Test
    public void testVisitShowTablesSchema() throws Exception {
        SelectAnalyzedStatement analyzedStatement = analyze("show tables in QNAME");

        assertThat(analyzedStatement.relation().querySpec(), isSQL(
            "SELECT information_schema.tables.table_name " +
            "WHERE (information_schema.tables.table_schema = 'qname') " +
            "GROUP BY information_schema.tables.table_name " +
            "ORDER BY information_schema.tables.table_name"));

        analyzedStatement = analyze("show tables");

        assertThat(analyzedStatement.relation().querySpec(), isSQL(
            "SELECT information_schema.tables.table_name " +
            "WHERE (NOT (information_schema.tables.table_schema = ANY(['information_schema', 'sys', 'pg_catalog']))) " +
            "GROUP BY information_schema.tables.table_name " +
            "ORDER BY information_schema.tables.table_name"));
    }

    @Test
    public void testVisitShowTablesLike() throws Exception {
        SelectAnalyzedStatement analyzedStatement = analyze("show tables in QNAME like 'likePattern'");

        assertThat(analyzedStatement.relation().querySpec(), isSQL(
            "SELECT information_schema.tables.table_name " +
            "WHERE ((information_schema.tables.table_schema = 'qname') " +
            "AND (information_schema.tables.table_name LIKE 'likePattern')) " +
            "GROUP BY information_schema.tables.table_name " +
            "ORDER BY information_schema.tables.table_name"));

        analyzedStatement = analyze("show tables like '%'");

        assertThat(analyzedStatement.relation().querySpec(), isSQL(
            "SELECT information_schema.tables.table_name " +
            "WHERE ((NOT (information_schema.tables.table_schema = ANY(['information_schema', 'sys', 'pg_catalog']))) " +
            "AND (information_schema.tables.table_name LIKE '%')) " +
            "GROUP BY information_schema.tables.table_name " +
            "ORDER BY information_schema.tables.table_name"));
    }

    @Test
    public void testVisitShowTablesWhere() throws Exception {
        SelectAnalyzedStatement analyzedStatement =
            analyze("show tables in QNAME where table_name = 'foo' or table_name like '%bar%'");

        assertThat(analyzedStatement.relation().querySpec(), isSQL(
            "SELECT information_schema.tables.table_name " +
            "WHERE ((information_schema.tables.table_schema = 'qname') " +
            "AND ((information_schema.tables.table_name = 'foo') OR (information_schema.tables.table_name LIKE '%bar%'))) " +
            "GROUP BY information_schema.tables.table_name " +
            "ORDER BY information_schema.tables.table_name"));

        analyzedStatement = analyze("show tables where table_name like '%'");

        assertThat(analyzedStatement.relation().querySpec(), isSQL(
            "SELECT information_schema.tables.table_name " +
            "WHERE ((NOT (information_schema.tables.table_schema = ANY(['information_schema', 'sys', 'pg_catalog']))) " +
            "AND (information_schema.tables.table_name LIKE '%')) " +
            "GROUP BY information_schema.tables.table_name " +
            "ORDER BY information_schema.tables.table_name"));
    }

    @Test
    public void testShowSchemasLike() throws Exception {
        SelectAnalyzedStatement analyzedStatement = analyze("show schemas like '%'");

        QuerySpec querySpec = analyzedStatement.relation().querySpec();
        assertThat(querySpec, isSQL("SELECT information_schema.schemata.schema_name " +
                                    "WHERE (information_schema.schemata.schema_name LIKE '%') " +
                                    "ORDER BY information_schema.schemata.schema_name"));
    }

    @Test
    public void testShowSchemasWhere() throws Exception {
        SelectAnalyzedStatement analyzedStatement = analyze("show schemas where schema_name = 'doc'");

        QuerySpec querySpec = analyzedStatement.relation().querySpec();
        assertThat(querySpec, isSQL("SELECT information_schema.schemata.schema_name " +
                                    "WHERE (information_schema.schemata.schema_name = 'doc') " +
                                    "ORDER BY information_schema.schemata.schema_name"));
    }

    @Test
    public void testShowSchemas() throws Exception {
        SelectAnalyzedStatement analyzedStatement = analyze("show schemas");

        QuerySpec querySpec = analyzedStatement.relation().querySpec();
        assertThat(querySpec, isSQL("SELECT information_schema.schemata.schema_name " +
                                    "ORDER BY information_schema.schemata.schema_name"));
    }

    @Test
    public void testShowColumnsLike() throws Exception {
        SelectAnalyzedStatement analyzedStatement = analyze("show columns from schemata in information_schema like '%'");
        QuerySpec querySpec = analyzedStatement.relation().querySpec();
        assertThat(querySpec, isSQL(
            "SELECT information_schema.columns.column_name, information_schema.columns.data_type" +
            " WHERE (((information_schema.columns.table_name = 'schemata')" +
            " AND (information_schema.columns.table_schema = 'information_schema'))" +
            " AND (information_schema.columns.column_name LIKE '%'))" +
            " ORDER BY information_schema.columns.column_name"));
    }

    @Test
    public void testShowColumnsWhere() throws Exception {
        SelectAnalyzedStatement analyzedStatement = analyze("show columns in schemata from information_schema"
                                                            + " where column_name = 'id'");

        QuerySpec querySpec = analyzedStatement.relation().querySpec();
        assertThat(querySpec, isSQL(
            "SELECT information_schema.columns.column_name, information_schema.columns.data_type" +
            " WHERE (((information_schema.columns.table_name = 'schemata')" +
            " AND (information_schema.columns.table_schema = 'information_schema'))" +
            " AND (information_schema.columns.column_name = 'id'))" +
            " ORDER BY information_schema.columns.column_name"));
    }

    @Test
    public void testShowColumnsLikeWithoutSpecifiedSchema() throws Exception {
        SelectAnalyzedStatement analyzedStatement = analyze("show columns in schemata like '%'");

        QuerySpec querySpec = analyzedStatement.relation().querySpec();
        assertThat(querySpec, isSQL(
            "SELECT information_schema.columns.column_name, information_schema.columns.data_type" +
            " WHERE (((information_schema.columns.table_name = 'schemata')" +
            " AND (information_schema.columns.table_schema = 'doc'))" +
            " AND (information_schema.columns.column_name LIKE '%'))" +
            " ORDER BY information_schema.columns.column_name"));
    }

    @Test
    public void testShowColumnsFromOneTable() throws Exception {
        SelectAnalyzedStatement analyzedStatement = analyze("show columns in schemata");

        QuerySpec querySpec = analyzedStatement.relation().querySpec();
        assertThat(querySpec, isSQL(
            "SELECT information_schema.columns.column_name, information_schema.columns.data_type" +
            " WHERE ((information_schema.columns.table_name = 'schemata')" +
            " AND (information_schema.columns.table_schema = 'doc'))" +
            " ORDER BY information_schema.columns.column_name"));
    }

}
