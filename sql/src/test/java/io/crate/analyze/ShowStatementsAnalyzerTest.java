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

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.operator.OperatorModule;
import io.crate.sql.tree.QualifiedName;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Module;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShowStatementsAnalyzerTest extends BaseAnalyzerTest {


    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public static final TableIdent SCHEMATA = new TableIdent(InformationSchemaInfo.NAME, "schemata");
    public static final TableInfo SCHEMATA_INFO = new TestingTableInfo.Builder(
            SCHEMATA, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
            .add("schema_name", DataTypes.STRING, null)
            .build();

    public static final TableIdent COLUMNS = new TableIdent(InformationSchemaInfo.NAME, "columns");
    public static final TableInfo COLUMNS_INFO = new TestingTableInfo.Builder(
            COLUMNS, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
            .add("column_name", DataTypes.STRING, null)
            .add("schema_name", DataTypes.STRING, null)
            .add("table_name", DataTypes.STRING, null)
            .build();

    public static final TableIdent TABLES = new TableIdent(InformationSchemaInfo.NAME, "tables");
    public static final TableInfo TABLES_INFO = new TestingTableInfo.Builder(
            TABLES, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
            .add("table_name", DataTypes.STRING, null)
            .add("schema_name", DataTypes.STRING, null)
            .build();

    public static final TableIdent CONSTRAINTS = new TableIdent(InformationSchemaInfo.NAME, "table_constraints");
    public static final TableInfo CONSTRAINTS_INFO = new TestingTableInfo.Builder(
            CONSTRAINTS, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
            .add("table_name", DataTypes.STRING, null)
            .add("schema_name", DataTypes.STRING, null)
            .add("data_type", DataTypes.STRING, null)
            .add("constraint_name", new ArrayType(DataTypes.STRING), null)
            .build();



    static class TestMetaDataModule extends MetaDataModule {

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(SCHEMATA.name())).thenReturn(SCHEMATA_INFO);
            when(schemaInfo.getTableInfo(COLUMNS.name())).thenReturn(COLUMNS_INFO);
            when(schemaInfo.getTableInfo(CONSTRAINTS.name())).thenReturn(CONSTRAINTS_INFO);
            when(schemaInfo.getTableInfo(TABLES.name())).thenReturn(TABLES_INFO);
            schemaBinder.addBinding(InformationSchemaInfo.NAME).toInstance(schemaInfo);
        }
    }

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                new MockedClusterServiceModule(),
                new TestMetaDataModule(),
                new OperatorModule()
        ));
        return modules;
    }

    @Test
    public void testVisitShowTablesSchema() throws Exception {

        SelectAnalyzedStatement analyzedStatement = analyze("show tables in QNAME");

        assertThat(analyzedStatement.relation().querySpec(), TestingHelpers.isSQL(
                "SELECT information_schema.tables.table_name " +
                "WHERE information_schema.tables.schema_name = 'qname' " +
                "ORDER BY information_schema.tables.table_name"));

        analyzedStatement = analyze("show tables");

        assertThat(analyzedStatement.relation().querySpec(), TestingHelpers.isSQL(
                "SELECT information_schema.tables.table_name " +
                "WHERE not information_schema.tables.schema_name = ANY(['information_schema', 'sys'])   " +
                "ORDER BY information_schema.tables.table_name"));
    }

    @Test
    public void testVisitShowTablesLike() throws Exception {

        SelectAnalyzedStatement analyzedStatement = analyze("show tables in QNAME like 'likePattern'");

        assertThat(analyzedStatement.relation().querySpec(), TestingHelpers.isSQL(
                "SELECT information_schema.tables.table_name " +
                "WHERE information_schema.tables.schema_name = 'qname' and information_schema.tables.table_name like 'likePattern' " +
                "ORDER BY information_schema.tables.table_name"));

        analyzedStatement = analyze("show tables like '%'");

        assertThat(analyzedStatement.relation().querySpec(), TestingHelpers.isSQL(
                "SELECT information_schema.tables.table_name " +
                "WHERE not information_schema.tables.schema_name = ANY(['information_schema', 'sys'])   " +
                "and information_schema.tables.table_name like '%' " +
                "ORDER BY information_schema.tables.table_name"));
    }

    @Test
    public void testVisitShowTablesWhere() throws Exception {

        SelectAnalyzedStatement analyzedStatement = analyze("show tables in QNAME where table_name = 'foo' or table_name like '%bar%'");

        assertThat(analyzedStatement.relation().querySpec(), TestingHelpers.isSQL(
                "SELECT information_schema.tables.table_name " +
                "WHERE information_schema.tables.schema_name = 'qname' " +
                "and information_schema.tables.table_name = 'foo' or information_schema.tables.table_name like '%bar%' " +
                "ORDER BY information_schema.tables.table_name"));

        analyzedStatement = analyze("show tables where table_name like '%'");

        assertThat(analyzedStatement.relation().querySpec(), TestingHelpers.isSQL(
                "SELECT information_schema.tables.table_name " +
                "WHERE not information_schema.tables.schema_name = ANY(['information_schema', 'sys'])   " +
                "and information_schema.tables.table_name like '%' " +
                "ORDER BY information_schema.tables.table_name"));
    }


    @Test
    public void testShowSchemasLike() throws Exception {
        SelectAnalyzedStatement analyzedStatement = analyze("show schemas like '%'");

        QuerySpec querySpec = analyzedStatement.relation().querySpec();
        assertThat(querySpec, TestingHelpers.isSQL("SELECT information_schema.schemata.schema_name " +
                                                   "WHERE information_schema.schemata.schema_name like '%' " +
                                                   "ORDER BY information_schema.schemata.schema_name"));
    }

    @Test
    public void testShowSchemasWhere() throws Exception {
        SelectAnalyzedStatement analyzedStatement = analyze("show schemas where schema_name = 'doc'");

        QuerySpec querySpec = analyzedStatement.relation().querySpec();
        assertThat(querySpec, TestingHelpers.isSQL("SELECT information_schema.schemata.schema_name " +
                                                   "WHERE information_schema.schemata.schema_name = 'doc' " +
                                                   "ORDER BY information_schema.schemata.schema_name"));
    }

    @Test
    public void testShowSchemas() throws Exception {
        SelectAnalyzedStatement analyzedStatement = analyze("show schemas");

        QuerySpec querySpec = analyzedStatement.relation().querySpec();
        assertThat(querySpec, TestingHelpers.isSQL("SELECT information_schema.schemata.schema_name " +
                                                   "ORDER BY information_schema.schemata.schema_name"));
    }

    public void testShowColumnsLike() throws Exception {
        SelectAnalyzedStatement analyzedStatement = analyze("show columns from schemata in information_schema like '%'");
        QuerySpec querySpec = ((MultiSourceSelect) analyzedStatement.relation()).sources()
                .get(new QualifiedName("cl")).querySpec();

        assertThat(querySpec, TestingHelpers.isSQL(
                "SELECT information_schema.columns.table_name," +
                " information_schema.columns.schema_name, information_schema.columns.column_name" +
                " WHERE information_schema.columns.table_name = 'schemata'" +
                " and information_schema.columns.schema_name = 'information_schema'" +
                " and information_schema.columns.column_name like '%'" +
                " ORDER BY information_schema.columns.column_name"));
    }

    public void testShowColumnsWhere() throws Exception {
        SelectAnalyzedStatement analyzedStatement = analyze("show columns in schemata from information_schema" +
                                                            " where column_name = 'id'");
        QuerySpec querySpec = ((MultiSourceSelect) analyzedStatement.relation()).sources()
                .get(new QualifiedName("cl")).querySpec();

        assertThat(querySpec, TestingHelpers.isSQL(
                "SELECT information_schema.columns.table_name, information_schema.columns.schema_name," +
                " information_schema.columns.column_name" +
                " WHERE information_schema.columns.table_name = 'schemata'" +
                " and information_schema.columns.schema_name = 'information_schema'" +
                " and information_schema.columns.column_name = 'id'" +
                " ORDER BY information_schema.columns.column_name"));
    }

    public void testShowColumnsLikeWithoutSpecifiedSchema() throws Exception {
        SelectAnalyzedStatement analyzedStatement = analyze("show columns in schemata like '%'");
        QuerySpec querySpec = ((MultiSourceSelect) analyzedStatement.relation()).sources()
                .get(new QualifiedName("cl")).querySpec();

        assertThat(querySpec, TestingHelpers.isSQL(
                "SELECT information_schema.columns.table_name, information_schema.columns.schema_name," +
                " information_schema.columns.column_name" +
                " WHERE information_schema.columns.table_name = 'schemata'" +
                " and information_schema.columns.schema_name = 'doc'" +
                " and information_schema.columns.column_name like '%'" +
                " ORDER BY information_schema.columns.column_name"));
    }

    public void testShowColumnsFromOneTable() throws Exception {
        SelectAnalyzedStatement analyzedStatement = analyze("show columns in schemata");
        QuerySpec querySpec = ((MultiSourceSelect) analyzedStatement.relation()).sources()
                .get(new QualifiedName("cl")).querySpec();

        assertThat(querySpec, TestingHelpers.isSQL(
                "SELECT information_schema.columns.table_name, information_schema.columns.schema_name," +
                " information_schema.columns.column_name" +
                " WHERE information_schema.columns.table_name = 'schemata'" +
                " and information_schema.columns.schema_name = 'doc'" +
                " ORDER BY information_schema.columns.column_name"));
    }

}
