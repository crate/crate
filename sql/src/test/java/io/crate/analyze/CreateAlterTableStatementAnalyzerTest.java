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

package io.crate.analyze;

import com.google.common.base.Joiner;
import io.crate.exceptions.*;
import io.crate.metadata.*;
import io.crate.metadata.information.MetaDataInformationModule;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.SchemaInfo;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.sql.parser.SqlParser;
import io.crate.testing.MockedClusterServiceModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.testing.TestingHelpers.mapToSortedString;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CreateAlterTableStatementAnalyzerTest extends BaseAnalyzerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void configure() {
            FulltextAnalyzerResolver fulltextAnalyzerResolver = mock(FulltextAnalyzerResolver.class);
            when(fulltextAnalyzerResolver.hasCustomAnalyzer("german")).thenReturn(false);
            when(fulltextAnalyzerResolver.hasCustomAnalyzer("ft_search")).thenReturn(true);
            Settings.Builder settingsBuilder = Settings.builder();
            settingsBuilder.put("search", "foobar");
            when(fulltextAnalyzerResolver.resolveFullCustomAnalyzerSettings("ft_search")).thenReturn(settingsBuilder.build());
            bind(FulltextAnalyzerResolver.class).toInstance(fulltextAnalyzerResolver);
            super.configure();
        }

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(USER_TABLE_IDENT.name())).thenReturn(USER_TABLE_INFO);
            when(schemaInfo.getTableInfo(USER_TABLE_REFRESH_INTERVAL_BY_ONLY.name())).thenReturn(USER_TABLE_INFO_REFRESH_INTERVAL_BY_ONLY);
            schemaBinder.addBinding(Schemas.DEFAULT_SCHEMA_NAME).toInstance(schemaInfo);
        }
    }

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                        new MockedClusterServiceModule(),
                        new MetaDataInformationModule(),
                        new TestMetaDataModule(),
                        new MetaDataSysModule(),
                        new OperatorModule(),
                        new ScalarFunctionModule())
        );
        return modules;
    }

    @Test
    public void testCreateTableWithAlternativePrimaryKeySyntax() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (id integer, name string, primary key (id, name))"
        );

        String[] primaryKeys = analysis.primaryKeys().toArray(new String[0]);
        assertThat(primaryKeys.length, is(2));
        assertThat(primaryKeys[0], is("id"));
        assertThat(primaryKeys[1], is("name"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSimpleCreateTable() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (id integer primary key, name string not null) " +
                "clustered into 3 shards with (number_of_replicas=0)");

        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.NUMBER_OF_SHARDS), is("3"));
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.NUMBER_OF_REPLICAS), is("0"));

        Map<String, Object> metaMapping = ((Map) analysis.mapping().get("_meta"));

        assertNull(metaMapping.get("columns"));

        Map<String,Object> mappingProperties = analysis.mappingProperties();

        Map<String, Object> idMapping = (Map<String, Object>)mappingProperties.get("id");
        assertThat((Boolean)idMapping.get("store"), is(false));
        assertThat((String)idMapping.get("type"), is("integer"));

        Map<String, Object> nameMapping = (Map<String, Object>)mappingProperties.get("name");
        assertThat((Boolean)nameMapping.get("store"), is(false));
        assertThat((String)nameMapping.get("type"), is("string"));

        String[] primaryKeys = analysis.primaryKeys().toArray(new String[0]);
        assertThat(primaryKeys.length, is(1));
        assertThat(primaryKeys[0], is("id"));

        String[] notNullColumns = analysis.notNullColumns().toArray(new String[0]);
        assertThat(notNullColumns.length, is(1));
        assertThat(notNullColumns[0], is("name"));
    }

    @Test
    public void testCreateTableWithDefaultNumberOfShards() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze("create table foo (id integer primary key, name string)");
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.NUMBER_OF_SHARDS), is("6"));
    }

    @Test
    public void testCreateTableWithDefaultNumberOfShardsWithClusterByClause() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (id integer primary key) clustered by (id)"
        );
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.NUMBER_OF_SHARDS), is("6"));
    }

    @Test
    public void testCreateTableNumberOfShardsProvidedInClusteredClause() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (id integer primary key) " +
                "clustered by (id) into 8 shards"
        );
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.NUMBER_OF_SHARDS), is("8"));
    }

    @Test
    public void testCreateTableWithRefreshInterval() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "CREATE TABLE foo (id int primary key, content string) " +
                        "with (refresh_interval=5000)");
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.REFRESH_INTERVAL), is("5000ms"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTableWithRefreshIntervalWrongNumberFormat() throws Exception {
        analyze("CREATE TABLE foo (id int primary key, content string) " +
                "with (refresh_interval='1asdf')");
    }

    @Test
    public void testAlterTableWithRefreshInterval() throws Exception {
        // alter t set
        AlterTableAnalyzedStatement analysisSet = analyze(
                "ALTER TABLE user_refresh_interval " +
                "SET (refresh_interval = '5000')");
        assertEquals("5000ms", analysisSet.tableParameter().settings().get(TableParameterInfo.REFRESH_INTERVAL));

        // alter t reset
        AlterTableAnalyzedStatement analysisReset = analyze(
                "ALTER TABLE user_refresh_interval " +
                "RESET (refresh_interval)");
        assertEquals("1000ms", analysisReset.tableParameter().settings().get(TableParameterInfo.REFRESH_INTERVAL));
    }

    @Test
    public void testAlterTableWithColumnPolicy() throws Exception {
        AlterTableAnalyzedStatement analysisSet = analyze(
                "ALTER TABLE user_refresh_interval " +
                        "SET (column_policy = 'strict')");
        assertEquals(ColumnPolicy.STRICT.mappingValue(), analysisSet.tableParameter().mappings().get(TableParameterInfo.COLUMN_POLICY));
    }

    @Test
    public void testAlterTableWithInvalidColumnPolicy() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'column_policy'");
        analyze("ALTER TABLE user_refresh_interval " +
                "SET (column_policy = 'ignored')");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithClusteredBy() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (id integer, name string) clustered by(id)");

        Map<String, Object> meta = (Map)analysis.mapping().get("_meta");
        assertNotNull(meta);
        assertThat((String) meta.get("routing"), is("id"));
    }

    @Test (expected = IllegalArgumentException.class)
    @SuppressWarnings("unchecked")
    public void testCreateTableWithClusteredByNotInPrimaryKeys() throws Exception {
        analyze("create table foo (id integer primary key, name string) clustered by(name)");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithObjects() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (id integer primary key, details object as (name string, age integer))");

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> details = (Map<String, Object>)mappingProperties.get("details");

        assertThat((String)details.get("type"), is("object"));
        assertThat((String)details.get("dynamic"), is("true"));

        Map<String, Object> detailsProperties = (Map<String, Object>)details.get("properties");
        Map<String, Object> nameProperties = (Map<String, Object>) detailsProperties.get("name");
        assertThat((String)nameProperties.get("type"), is("string"));

        Map<String, Object> ageProperties = (Map<String, Object>) detailsProperties.get("age");
        assertThat((String) ageProperties.get("type"), is("integer"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithStrictObject() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (id integer primary key, details object(strict) as (name string, age integer))");

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> details = (Map<String, Object>)mappingProperties.get("details");

        assertThat((String)details.get("type"), is("object"));
        assertThat((String) details.get("dynamic"), is("strict"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithIgnoredObject() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (id integer primary key, details object(ignored))");

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> details = (Map<String, Object>)mappingProperties.get("details");

        assertThat((String) details.get("type"), is("object"));
        assertThat((String) details.get("dynamic"), is("false"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithSubscriptInFulltextIndexDefinition() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table my_table1g ("+
                        "title string, " +
                        "author object(dynamic) as ( " +
                            "name string, " +
                            "birthday timestamp " +
                        "), " +
                "INDEX author_title_ft using fulltext(title, author['name']))");

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> details = (Map<String, Object>)mappingProperties.get("author");
        Map<String, Object> nameMapping = (Map<String, Object>)((Map<String, Object>) details.get("properties")).get("name");

        assertThat(((List<String>) nameMapping.get("copy_to")).get(0), is("author_title_ft"));
    }

    @Test (expected = ColumnUnknownException.class)
    public void testCreateTableWithInvalidFulltextIndexDefinition() throws Exception {
        analyze("create table my_table1g (" +
                "title string, " +
                "author object(dynamic) as ( " +
                "name string, " +
                "birthday timestamp " +
                "), " +
                "INDEX author_title_ft using fulltext(title, author['name']['foo']['bla']))");
    }

    @Test
    public void testCreateTableWithArray() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (id integer primary key, details array(string))");
        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> details = (Map<String, Object>)mappingProperties.get("details");

        assertThat((String)details.get("type"), is("array"));
        Map<String, Object> inner = (Map<String, Object>)details.get("inner");
        assertThat((String) inner.get("type"), is("string"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithObjectsArray() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (id integer primary key, details array(object as (name string, age integer, tags array(string))))");

        Map<String, Object> metaMapping = (Map) analysis.mapping().get("_meta");
        assertThat(Joiner.on(", ").withKeyValueSeparator(":").join(metaMapping), is("primary_keys:[id]"));

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        assertThat(mapToSortedString(mappingProperties),
                is("details={inner={dynamic=true, " +
                        "properties={age={doc_values=true, index=not_analyzed, store=false, type=integer}, " +
                        "name={doc_values=true, index=not_analyzed, store=false, type=string}, " +
                        "tags={inner={doc_values=false, index=not_analyzed, store=false, type=string}, type=array}}," +
                        " type=object}, type=array}, " +
                        "id={doc_values=true, index=not_analyzed, store=false, type=integer}"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithAnalyzer() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (id integer primary key, content string INDEX using fulltext with (analyzer='german'))");

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> contentMapping = (Map<String, Object>)mappingProperties.get("content");

        assertThat((String)contentMapping.get("index"), is("analyzed"));
        assertThat((String) contentMapping.get("analyzer"), is("german"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithAnalyzerParameter() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (id integer primary key, content string INDEX using fulltext with (analyzer=?))",
                new Object[] {"german"}
        );

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> contentMapping = (Map<String, Object>)mappingProperties.get("content");

        assertThat((String)contentMapping.get("index"), is("analyzed"));
        assertThat((String)contentMapping.get("analyzer"), is("german"));
    }

    @Test
    public void textCreateTableWithCustomAnalyzerInNestedColumn() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table ft_search (" +
                    "user object (strict) as (" +
                        "name string index using fulltext with (analyzer='ft_search') " +
                    ")"+
                ")");
        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> details = (Map<String, Object>)mappingProperties.get("user");
        Map<String, Object> nameMapping = (Map<String, Object>)((Map<String, Object>) details.get("properties")).get("name");

        assertThat((String) nameMapping.get("index"), is("analyzed"));
        assertThat((String) nameMapping.get("analyzer"), is("ft_search"));

        assertThat(analysis.tableParameter().settings().get("search"), is("foobar"));
    }

    @Test
    public void testCreateTableWithSchemaName() throws Exception {
        CreateTableAnalyzedStatement analysis =
                analyze("create table something.foo (id integer primary key)");
        TableIdent tableIdent = analysis.tableIdent();
        assertThat(tableIdent.schema(), is("something"));
        assertThat(tableIdent.name(), is("foo"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithIndexColumn() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (id integer primary key, content string, INDEX content_ft using fulltext (content))");

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> contentMapping = (Map<String, Object>)mappingProperties.get("content");

        assertThat((String)contentMapping.get("index"), is("not_analyzed"));
        assertThat(((List<String>)contentMapping.get("copy_to")).get(0), is("content_ft"));

        Map<String, Object> ft_mapping = (Map<String, Object>)mappingProperties.get("content_ft");
        assertThat((String)ft_mapping.get("index"), is("analyzed"));
        assertThat((String) ft_mapping.get("analyzer"), is("standard"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithPlainIndexColumn() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (id integer primary key, content string, INDEX content_ft using plain (content))");

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> contentMapping = (Map<String, Object>)mappingProperties.get("content");

        assertThat((String)contentMapping.get("index"), is("not_analyzed"));
        assertThat(((List<String>)contentMapping.get("copy_to")).get(0), is("content_ft"));

        Map<String, Object> ft_mapping = (Map<String, Object>)mappingProperties.get("content_ft");
        assertThat((String)ft_mapping.get("index"), is("analyzed"));
        assertThat((String) ft_mapping.get("analyzer"), is("keyword"));
    }

    @Test
    public void testCreateTableWithIndexColumnOverNonString() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("INDEX definition only support 'string' typed source columns");
        analyze("create table foo (id integer, id2 integer, INDEX id_ft using fulltext (id, id2))");
    }

    @Test
    public void testCreateTableWithIndexColumnOverNonString2() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("INDEX definition only support 'string' typed source columns");
        analyze("create table foo (id integer, name string, INDEX id_ft using fulltext (id, name))");
   }

    @Test
    public void testChangeNumberOfReplicas() throws Exception {
        AlterTableAnalyzedStatement analysis =
                analyze("alter table users set (number_of_replicas=2)");

        assertThat(analysis.table().ident().name(), is("users"));
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.NUMBER_OF_REPLICAS), is("2"));
    }

    @Test
    public void testResetNumberOfReplicas() throws Exception {
        AlterTableAnalyzedStatement analysis =
                analyze("alter table users reset (number_of_replicas)");

        assertThat(analysis.table().ident().name(), is("users"));
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.NUMBER_OF_REPLICAS), is("1"));
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.AUTO_EXPAND_REPLICAS), is("false"));
    }

    @Test (expected = IllegalArgumentException.class)
    public void testAlterTableWithInvalidProperty() throws Exception {
        analyze("alter table users set (foobar='2')");
    }

    @Test
    public void testAlterSystemTable() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("The table sys.shards is read-only. Write, Drop or Alter operations are not supported");
        analyze("alter table sys.shards reset (number_of_replicas)");
    }

    @Test
    public void testCreateTableWithMultiplePrimaryKeys() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table test (id integer primary key, name string primary key)");

        String[] primaryKeys = analysis.primaryKeys().toArray(new String[0]);
        assertThat(primaryKeys.length, is(2));
        assertThat(primaryKeys[0], is("id"));
        assertThat(primaryKeys[1], is("name"));
    }

    @Test
    public void testCreateTableWithMultiplePrimaryKeysAndClusteredBy() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table test (id integer primary key, name string primary key) " +
                        "clustered by(name)");

        String[] primaryKeys = analysis.primaryKeys().toArray(new String[0]);
        assertThat(primaryKeys.length, is(2));
        assertThat(primaryKeys[0], is("id"));
        assertThat(primaryKeys[1], is("name"));

        Map<String, Object> meta = (Map)analysis.mapping().get("_meta");
        assertNotNull(meta);
        assertThat((String) meta.get("routing"), is("name"));

    }

    @Test (expected = IllegalArgumentException.class)
    public void testCreateTableWithSystemColumnPrefix() throws Exception {
        analyze("create table test (_id integer, name string)");
    }

    @Test(expected = InvalidTableNameException.class)
    public void testCreateTableIllegalTableName() throws Exception {
        analyze("create table \"abc.def\" (id integer primary key, name string)");
    }

    @Test
    public void testHasColumnDefinition() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table my_table (" +
                "  id integer primary key, " +
                "  name string, " +
                "  indexed string index using fulltext with (analyzer='german')," +
                "  arr array(object as(" +
                "    nested float," +
                "    nested_object object as (id byte)" +
                "  ))," +
                "  obj object as ( content string )," +
                "  index ft using fulltext(name, obj['content']) with (analyzer='standard')" +
                ")");
        assertTrue(analysis.hasColumnDefinition(ColumnIdent.fromPath("id")));
        assertTrue(analysis.hasColumnDefinition(ColumnIdent.fromPath("name")));
        assertTrue(analysis.hasColumnDefinition(ColumnIdent.fromPath("indexed")));
        assertTrue(analysis.hasColumnDefinition(ColumnIdent.fromPath("arr")));
        assertTrue(analysis.hasColumnDefinition(ColumnIdent.fromPath("arr.nested")));
        assertTrue(analysis.hasColumnDefinition(ColumnIdent.fromPath("arr.nested_object.id")));
        assertTrue(analysis.hasColumnDefinition(ColumnIdent.fromPath("obj")));
        assertTrue(analysis.hasColumnDefinition(ColumnIdent.fromPath("obj.content")));

        assertFalse(analysis.hasColumnDefinition(ColumnIdent.fromPath("arr.nested.wrong")));
        assertFalse(analysis.hasColumnDefinition(ColumnIdent.fromPath("ft")));
        assertFalse(analysis.hasColumnDefinition(ColumnIdent.fromPath("obj.content.ft")));
    }

    @Test
    public void testCreateTableWithGeoPoint() throws Exception {
        CreateTableAnalyzedStatement analyze = analyze(
                "create table geo_point_table (\n" +
                "    id integer primary key,\n" +
                "    my_point geo_point\n" +
                ")\n");
        Map my_point = (Map) analyze.mappingProperties().get("my_point");
        assertEquals("geo_point", my_point.get("type"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testClusteredIntoZeroShards() throws Exception {
        analyze("create table my_table (" +
                "  id integer," +
                "  name string" +
                ") clustered into 0 shards");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBlobTableClusteredIntoZeroShards() throws Exception {
        analyze("create blob table my_table " +
                "clustered into 0 shards");
    }

    @Test
    public void testEarlyPrimaryKeyConstraint() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table my_table (" +
                "primary key (id1, id2)," +
                "id1 integer," +
                "id2 long" +
                ")");
        assertThat(analysis.primaryKeys().size(), is(2));
        assertThat(analysis.primaryKeys(), hasItems("id1", "id2"));
    }

    @Test(expected = ColumnUnknownException.class)
    public void testPrimaryKeyConstraintNonExistingColumns() throws Exception {
        analyze("create table my_table (" +
                "primary key (id1, id2)," +
                "title string," +
                "name string" +
                ")");
    }

    @Test
    public void testEarlyIndexDefinition() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table my_table (" +
                "index ft using fulltext(title, name) with (analyzer='snowball')," +
                "title string," +
                "name string" +
                ")");
        assertThat(
                Joiner.on(", ").withKeyValueSeparator(": ").join((Map)analysis.mapping().get("_meta")),
                is("indices: {ft={}}"));
        assertThat(
                (List<String>) ((Map<String, Object>) analysis.mappingProperties()
                        .get("title")).get("copy_to"),
                hasItem("ft")
        );
        assertThat(
                (List<String>) ((Map<String, Object>) analysis.mappingProperties()
                        .get("name")).get("copy_to"),
                hasItem("ft"));

    }

    @Test(expected = ColumnUnknownException.class)
    public void testIndexDefinitionNonExistingColumns() throws Exception {
        analyze("create table my_table (" +
                "index ft using fulltext(id1, id2) with (analyzer='snowball')," +
                "title string," +
                "name string" +
                ")");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testAnalyzerOnInvalidType() throws Exception {
        analyze("create table my_table (x integer INDEX using fulltext with (analyzer='snowball'))");
    }

    @Test
    public void createTableNegativeReplicas() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table t (id int, name string) with (number_of_replicas=-1)");
        assertThat(analysis.tableParameter().settings().getAsInt(TableParameterInfo.NUMBER_OF_REPLICAS, 0), is(-1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTableSameColumn() throws Exception {
        analyze("create table my_table (title string, title integer)");
    }


    @Test (expected = UnsupportedOperationException.class)
    public void testCreateTableWithArrayPrimaryKeyUnsupported() throws Exception {
        analyze("create table t (id array(int) primary key)");
    }

    @Test
    public void testCreateTableWithClusteredIntoShardsParameter() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table t (id int primary key) clustered into ? shards", new Object[]{2});
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.NUMBER_OF_SHARDS), is("2"));
    }

    @Test
    public void testCreateTableWithClusteredIntoShardsParameterNonNumeric() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid number 'foo'");
        analyze("create table t (id int primary key) clustered into ? shards", new Object[]{"foo"});
    }

    @Test
    public void testCreateTableWithParitionedColumnInClusteredBy() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot use CLUSTERED BY column in PARTITIONED BY clause");
        analyze("create table t(id int primary key) partitioned by (id) clustered by (id)");
    }

    @Test
    public void testCreateTableWithEmptySchema() throws Exception {
        expectedException.expect(InvalidSchemaNameException.class);
        expectedException.expectMessage("schema name \"\" is invalid.");
        analyze("create table \"\".my_table (" +
                "id long primary key" +
                ")");
    }

    @Test
    public void testCreateTableWithIllegalSchema() throws Exception {
        expectedException.expect(InvalidSchemaNameException.class);
        expectedException.expectMessage("schema name \"with.\" is invalid.");
        analyze("create table \"with.\".my_table (" +
                "id long primary key" +
                ")");
    }

    @Test
    public void testCreateTableWithInvalidColumnName() throws Exception {
        expectedException.expect(InvalidColumnNameException.class);
        expectedException.expectMessage("column name \"'test\" is invalid");
        analyze("create table my_table (\"'test\" string)");
    }

    @Test
    public void testInvalidColumnNamePredicate() throws Exception {
        assertThat(ColumnIdent.INVALID_COLUMN_NAME_PREDICATE.apply("validName"), is(false));
        assertThat(ColumnIdent.INVALID_COLUMN_NAME_PREDICATE.apply("invalid["), is(true));
        assertThat(ColumnIdent.INVALID_COLUMN_NAME_PREDICATE.apply("invalid'"), is(true));
        assertThat(ColumnIdent.INVALID_COLUMN_NAME_PREDICATE.apply("invalid]"), is(true));
        assertThat(ColumnIdent.INVALID_COLUMN_NAME_PREDICATE.apply("invalid."), is(true));
    }

    @Test
    public void testCreateTableShouldRaiseErrorIfItExists() throws Exception {
        expectedException.expect(TableAlreadyExistsException.class);
        analyze("create table users (\"'test\" string)");
    }

    @Test
    public void testExplicitSchemaHasPrecedenceOverDefaultSchema() throws Exception {
        CreateTableAnalyzedStatement statement = (CreateTableAnalyzedStatement) analyzer.analyze(
                SqlParser.createStatement("create table foo.bar (x string)"),
                new ParameterContext(new Object[0], new Object[0][], "hoschi")).analyzedStatement();

        // schema from statement must take precedence
        assertThat(statement.tableIdent().schema(), is("foo"));
    }

    @Test
    public void testDefaultSchemaIsAddedToTableIdentIfNoEplicitSchemaExistsInTheStatement() throws Exception {
        CreateTableAnalyzedStatement statement = (CreateTableAnalyzedStatement) analyzer.analyze(
                SqlParser.createStatement("create table bar (x string)"),
                new ParameterContext(new Object[0], new Object[0][], "hoschi")).analyzedStatement();

        assertThat(statement.tableIdent().schema(), is("hoschi"));
    }

    @Test
    public void testChangeReadBlock() throws Exception {
        AlterTableAnalyzedStatement analysis =
                analyze("alter table users set (\"blocks.read\"=true)");
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.BLOCKS_READ), is("true"));
    }

    @Test
    public void testChangeWriteBlock() throws Exception {
        AlterTableAnalyzedStatement analysis =
                analyze("alter table users set (\"blocks.write\"=true)");
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.BLOCKS_WRITE), is("true"));
    }

    @Test
    public void testChangeMetadataBlock() throws Exception {
        AlterTableAnalyzedStatement analysis =
                analyze("alter table users set (\"blocks.metadata\"=true)");
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.BLOCKS_METADATA), is("true"));
    }

    @Test
    public void testChangeReadOnlyBlock() throws Exception {
        AlterTableAnalyzedStatement analysis =
                analyze("alter table users set (\"blocks.read_only\"=true)");
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.READ_ONLY), is("true"));
    }

    @Test
    public void testChangeFlushThresholdOpsNumber() throws Exception {
        AlterTableAnalyzedStatement analysis =
                analyze("alter table users set (\"translog.flush_threshold_ops\"=10)");
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.FLUSH_THRESHOLD_OPS), is("10"));
    }

    @Test
    public void testChangeFlushThresholdSize() throws Exception {
        AlterTableAnalyzedStatement analysis =
                analyze("alter table users set (\"translog.flush_threshold_size\"=300)");
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.FLUSH_THRESHOLD_SIZE), is("300b"));
    }

    @Test
    public void testChangeFlushThresholdPeriod() throws Exception {
        AlterTableAnalyzedStatement analysis =
                analyze("alter table users set (\"translog.flush_threshold_period\"=35)");
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.FLUSH_THRESHOLD_PERIOD), is("35ms"));
    }

    @Test
    public void testChangeFlushDisable() throws Exception {
        AlterTableAnalyzedStatement analysis =
                analyze("alter table users set (\"translog.disable_flush\"=true)");
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.FLUSH_DISABLE), is("true"));
    }

    @Test
    public void testChangeTranslogInterval() throws Exception {
        AlterTableAnalyzedStatement analysis =
                analyze("alter table users set (\"translog.interval\"=50)");
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.TRANSLOG_INTERVAL), is("50ms"));
    }

    @Test
    public void testRoutingAllocationEnable() throws Exception {
        AlterTableAnalyzedStatement analysis =
                analyze("alter table users set (\"routing.allocation.enable\"=\"none\")");
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.ROUTING_ALLOCATION_ENABLE), is("none"));
    }

    @Test
    public void testRoutingAllocationValidation() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        analyze("alter table users set (\"routing.allocation.enable\"=\"foo\")");
    }

    @Test
    public void testRecoveryShardsWithString() throws Exception {
        AlterTableAnalyzedStatement analysis =
                analyze("alter table users set (\"recovery.initial_shards\"=\"full\")");
        assertThat(analysis.table().ident().name(), is("users"));
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.RECOVERY_INITIAL_SHARDS), is("full"));

    }

    @Test
    public void testRecoveryShardsWithInteger() throws Exception {
        AlterTableAnalyzedStatement analysis =
                analyze("alter table users set (\"recovery.initial_shards\"=1)");
        assertThat(analysis.table().ident().name(), is("users"));
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.RECOVERY_INITIAL_SHARDS), is("1"));

    }

    @Test
    public void testTranslogSyncInterval() throws Exception {

        AlterTableAnalyzedStatement analysis =
                analyze("alter table users set (\"translog.sync_interval\"='1s')");
        assertThat(analysis.table().ident().name(), is("users"));
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.TRANSLOG_SYNC_INTERVAL), is("1000ms"));

    }

    @Test
    public void testRecoveryShardsValidation() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        analyze("alter table users set (\"recovery.initial_shards\"=\"foo\")");
    }

    @Test
    public void testCreateReadOnlyTable() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (id integer primary key, name string) "
                + "clustered into 3 shards with (\"blocks.read_only\"=true)");
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.READ_ONLY), is("true"));
    }

    @Test
    public void testCreateTableWithGeneratedColumn() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (ts timestamp, day as date_trunc('day', ts))");

        Map<String, Object> metaMapping = ((Map) analysis.mapping().get("_meta"));
        assertThat(metaMapping.size(), is(1));
        Map<String, String> generatedColumnsMapping = (Map<String, String>) metaMapping.get("generated_columns");
        assertThat(generatedColumnsMapping.size(), is(1));
        assertThat(generatedColumnsMapping.get("day"), is("date_trunc('day', ts)"));

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        assertThat(mappingProperties.size(), is(2));
        Map<String, Object> dayMapping = (Map<String, Object>)mappingProperties.get("day");
        assertThat((String) dayMapping.get("type"), is("date"));
        Map<String, Object> tsMapping = (Map<String, Object>)mappingProperties.get("ts");
        assertThat((String) tsMapping.get("type"), is("date"));
    }

    @Test
    public void testCreateTableGeneratedColumnWithCast() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (ts timestamp, day timestamp GENERATED ALWAYS as ts + 1)");
        Map<String, Object> metaMapping = ((Map) analysis.mapping().get("_meta"));
        Map<String, String> generatedColumnsMapping = (Map<String, String>) metaMapping.get("generated_columns");
        assertThat(generatedColumnsMapping.get("day"), is("cast((ts + 1) AS timestamp)"));

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> dayMapping = (Map<String, Object>)mappingProperties.get("day");
        assertThat((String) dayMapping.get("type"), is("date"));
    }

    @Test
    public void testCreateTableGeneratedColumnWithSubscript() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (user object as (name string), name as concat(user['name'], 'foo'))");

        Map<String, Object> metaMapping = ((Map) analysis.mapping().get("_meta"));
        Map<String, String> generatedColumnsMapping = (Map<String, String>) metaMapping.get("generated_columns");
        assertThat(generatedColumnsMapping.get("name"), is("concat(user['name'], 'foo')"));
    }

    @Test
    public void testCreateTableGeneratedColumnParameter() throws Exception {
        CreateTableAnalyzedStatement analysis = analyze(
                "create table foo (user object as (name string), name as concat(user['name'], ?))", $("foo"));
        Map<String, Object> metaMapping = ((Map) analysis.mapping().get("_meta"));
        Map<String, String> generatedColumnsMapping = (Map<String, String>) metaMapping.get("generated_columns");
        assertThat(generatedColumnsMapping.get("name"), is("concat(user['name'], 'foo')"));
    }

    @Test
    public void testCreateTableGeneratedColumnWithInvalidType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("generated expression value type 'timestamp' not supported for conversion to 'string'");
        analyze("create table foo (ts timestamp, day string GENERATED ALWAYS as date_trunc('day', ts))");
    }

    @Test
    public void testCreateTableGeneratedColumnWithMatch() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("can only MATCH on columns, not on name");
        analyze("create table foo (name string, bar as match(name, 'crate'))");
    }

    @Test
    public void testCreateTableGeneratedColumnBasedOnGeneratedColumn() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A generated column cannot be based on a generated column");
        analyze("create table foo (ts timestamp, day as date_trunc('day', ts), date_string as cast(day as string))");
    }

    @Test
    public void testCreateTableGeneratedColumnBasedOnUnknownColumn() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column unknown_col unknown");
        analyze("create table foo (ts timestamp, day as date_trunc('day', ts), date_string as cast(unknown_col as string))");
    }

    @Test
    public void testCreateTableWithObjectAsPrimaryKey() throws Exception {
        expectedException.expectMessage("Cannot use columns of type \"object\" as primary key");
        expectedException.expect(UnsupportedOperationException.class);
        analyze("create table t (obj object as (x int) primary key)");
    }

    @Test
    public void testCreateTableWithGeoPointAsPrimaryKey() throws Exception {
        expectedException.expectMessage("Cannot use columns of type \"geo_point\" as primary key");
        expectedException.expect(UnsupportedOperationException.class);
        analyze("create table t (c geo_point primary key)");
    }

    @Test
    public void testCreateTableWithGeoShapeAsPrimaryKey() throws Exception {
        expectedException.expectMessage("Cannot use columns of type \"geo_shape\" as primary key");
        expectedException.expect(UnsupportedOperationException.class);
        analyze("create table t (c geo_shape primary key)");
    }

    @Test
    public void testCreateTableWithDuplicatePrimaryKey() throws Exception {
        assertDuplicatePrimaryKey("create table t (id int, primary key (id, id))");
        assertDuplicatePrimaryKey("create table t (obj object as (id int), primary key (obj['id'], obj['id']))");
        assertDuplicatePrimaryKey("create table t (id int primary key, primary key (id))");
        assertDuplicatePrimaryKey("create table t (obj object as (id int primary key), primary key (obj['id']))");
    }

    private void assertDuplicatePrimaryKey(String stmt) throws Exception {
        try {
            analyze(stmt);
            fail(String.format(Locale.ENGLISH, "Statement '%s' did not result in duplicate primary key exception", stmt));
        } catch (IllegalArgumentException e) {
            String msg = "appears twice in primary key constraint";
            if (!e.getMessage().contains(msg)) {
                fail("Exception message is expected to contain: " + msg);
            }
        }
    }

    @Test
    public void testCreateTableWithPrimaryKeyConstraintInArrayItem() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot use column \"id\" as primary key within an array object");
        analyze("create table test (arr array(object as (id long primary key)))");
    }

    @Test
    public void testCreateTableWithDeepNestedPrimaryKeyConstraintInArrayItem() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot use column \"name\" as primary key within an array object");
        analyze("create table test (arr array(object as (user object as (name string primary key), id long)))");
    }
}
