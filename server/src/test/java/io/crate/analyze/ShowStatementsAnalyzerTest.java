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

import static io.crate.testing.Asserts.isAlias;
import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;
import static io.crate.testing.Asserts.isReference;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.Asserts;
import io.crate.testing.SQLExecutor;

public class ShowStatementsAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor executor;

    @Before
    public void prepare() {
        executor = SQLExecutor.builder(clusterService).build();
    }

    private <T extends AnalyzedStatement> T analyze(String stmt) {
        return executor.analyze(stmt);
    }

    @Test
    public void testVisitShowTablesSchema() throws Exception {
        QueriedSelectRelation relation = analyze("show tables in QNAME");

        assertThat(relation.isDistinct(), is(true));
        assertThat(relation, isSQL(
            "SELECT information_schema.tables.table_name AS table_name " +
            "WHERE ((information_schema.tables.table_type = 'BASE TABLE') " +
            "AND (information_schema.tables.table_schema = 'qname')) " +
            "ORDER BY information_schema.tables.table_name AS table_name"));

        relation = analyze("show tables");
        assertThat(relation.isDistinct(), is(true));
        assertThat(relation, isSQL(
            "SELECT information_schema.tables.table_name AS table_name " +
            "WHERE ((information_schema.tables.table_type = 'BASE TABLE') " +
            "AND (NOT (information_schema.tables.table_schema = ANY(['information_schema', 'sys', 'pg_catalog'])))) " +
            "ORDER BY information_schema.tables.table_name AS table_name"));
    }

    @Test
    public void testVisitShowTablesLike() throws Exception {
        QueriedSelectRelation relation = analyze("show tables in QNAME like 'likePattern'");

        assertThat(relation.isDistinct(), is(true));
        assertThat(relation, isSQL(
            "SELECT information_schema.tables.table_name AS table_name " +
            "WHERE (((information_schema.tables.table_type = 'BASE TABLE') AND (information_schema.tables.table_schema = 'qname')) " +
            "AND (information_schema.tables.table_name LIKE 'likePattern')) " +
            "ORDER BY information_schema.tables.table_name AS table_name"));

        relation = analyze("show tables like '%'");
        assertThat(relation.isDistinct(), is(true));
        assertThat(relation, isSQL(
            "SELECT information_schema.tables.table_name AS table_name " +
            "WHERE (((information_schema.tables.table_type = 'BASE TABLE') AND " +
            "(NOT (information_schema.tables.table_schema = ANY(['information_schema', 'sys', 'pg_catalog'])))) " +
            "AND (information_schema.tables.table_name LIKE '%')) " +
            "ORDER BY information_schema.tables.table_name AS table_name"));
    }

    @Test
    public void testVisitShowTablesWhere() throws Exception {
        QueriedSelectRelation relation =
            analyze("show tables in QNAME where table_name = 'foo' or table_name like '%bar%'");
        assertThat(relation.isDistinct(), is(true));
        Asserts.assertThat(relation.outputs())
            .satisfiesExactly(
                isAlias("table_name", isReference("table_name")));
        Asserts.assertThat(relation.where())
            .isFunction(
                "op_and",
                isFunction("op_and"),
                isFunction("op_or", isFunction("op_="), isFunction("op_like")));
        Asserts.assertThat(relation.orderBy().orderBySymbols())
            .satisfiesExactly(isAlias("table_name", isReference("table_name")));

        relation = analyze("show tables where table_name like '%'");
        assertThat(relation.isDistinct(), is(true));
        assertThat(relation, isSQL(
            "SELECT information_schema.tables.table_name AS table_name " +
            "WHERE (((information_schema.tables.table_type = 'BASE TABLE') " +
            "AND (NOT (information_schema.tables.table_schema = ANY(['information_schema', 'sys', 'pg_catalog'])))) " +
            "AND (information_schema.tables.table_name LIKE '%')) " +
            "ORDER BY information_schema.tables.table_name AS table_name"));
    }

    @Test
    public void testShowSchemasLike() throws Exception {
        AnalyzedRelation relation = analyze("show schemas like '%'");
        assertThat(relation, isSQL("SELECT information_schema.schemata.schema_name " +
                                    "WHERE (information_schema.schemata.schema_name LIKE '%') " +
                                    "ORDER BY information_schema.schemata.schema_name"));
    }

    @Test
    public void testShowSchemasWhere() throws Exception {
        AnalyzedRelation relation = analyze("show schemas where schema_name = 'doc'");
        assertThat(relation, isSQL("SELECT information_schema.schemata.schema_name " +
                                    "WHERE (information_schema.schemata.schema_name = 'doc') " +
                                    "ORDER BY information_schema.schemata.schema_name"));
    }

    @Test
    public void testShowSchemas() throws Exception {
        AnalyzedRelation relation = analyze("show schemas");
        assertThat(relation, isSQL("SELECT information_schema.schemata.schema_name " +
                                    "ORDER BY information_schema.schemata.schema_name"));
    }

    @Test
    public void testShowColumnsLike() throws Exception {
        AnalyzedRelation relation = analyze("show columns from schemata in information_schema like '%'");
        assertThat(relation, isSQL(
            "SELECT information_schema.columns.column_name, information_schema.columns.data_type" +
            " WHERE (((information_schema.columns.table_name = 'schemata')" +
            " AND (information_schema.columns.table_schema = 'information_schema'))" +
            " AND (information_schema.columns.column_name LIKE '%'))" +
            " ORDER BY information_schema.columns.column_name"));
    }

    @Test
    public void testShowColumnsWhere() throws Exception {
        AnalyzedRelation relation = analyze("show columns in schemata from information_schema"
                                            + " where column_name = 'id'");
        assertThat(relation, isSQL(
            "SELECT information_schema.columns.column_name, information_schema.columns.data_type" +
            " WHERE (((information_schema.columns.table_name = 'schemata')" +
            " AND (information_schema.columns.table_schema = 'information_schema'))" +
            " AND (information_schema.columns.column_name = 'id'))" +
            " ORDER BY information_schema.columns.column_name"));
    }

    @Test
    public void testShowColumnsLikeWithoutSpecifiedSchema() throws Exception {
        AnalyzedRelation relation = analyze("show columns in schemata like '%'");
        assertThat(relation, isSQL(
            "SELECT information_schema.columns.column_name, information_schema.columns.data_type" +
            " WHERE (((information_schema.columns.table_name = 'schemata')" +
            " AND (information_schema.columns.table_schema = 'doc'))" +
            " AND (information_schema.columns.column_name LIKE '%'))" +
            " ORDER BY information_schema.columns.column_name"));
    }

    @Test
    public void testShowColumnsFromOneTable() throws Exception {
        AnalyzedRelation relation = analyze("show columns in schemata");
        assertThat(relation, isSQL(
            "SELECT information_schema.columns.column_name, information_schema.columns.data_type" +
            " WHERE ((information_schema.columns.table_name = 'schemata')" +
            " AND (information_schema.columns.table_schema = 'doc'))" +
            " ORDER BY information_schema.columns.column_name"));
    }

    @Test
    public void testRewriteOfTransactionIsolation() {
        QueriedSelectRelation stmt = analyze("show transaction isolation level");
        assertThat(stmt.from(), contains(instanceOf(TableFunctionRelation.class)));
        Asserts.assertThat(stmt.outputs())
            .satisfiesExactly(isAlias("transaction_isolation", isLiteral("read uncommitted")));

        stmt = analyze("show transaction_isolation");
        assertThat(stmt.from(), contains(instanceOf(TableFunctionRelation.class)));
        Asserts.assertThat(stmt.outputs())
            .satisfiesExactly(isAlias("transaction_isolation", isLiteral("read uncommitted")));
    }
}
