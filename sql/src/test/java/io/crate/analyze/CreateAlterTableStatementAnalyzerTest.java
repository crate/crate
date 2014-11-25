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
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.InvalidTableNameException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.information.MetaDataInformationModule;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.SchemaInfo;
import io.crate.operation.operator.OperatorModule;
import io.crate.testing.MockedClusterServiceModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
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
            ImmutableSettings.Builder settingsBuilder = ImmutableSettings.builder();
            settingsBuilder.put("search", "foobar");
            when(fulltextAnalyzerResolver.resolveFullCustomAnalyzerSettings("ft_search")).thenReturn(settingsBuilder.build());
            bind(FulltextAnalyzerResolver.class).toInstance(fulltextAnalyzerResolver);
            super.configure();
        }

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(TEST_DOC_TABLE_IDENT.name())).thenReturn(userTableInfo);
            when(schemaInfo.getTableInfo(TEST_DOC_TABLE_REFRESH_INTERVAL_BY_ONLY.name())).thenReturn(userTableInfoRefreshIntervalByOnly);
            schemaBinder.addBinding(DocSchemaInfo.NAME).toInstance(schemaInfo);
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
                        new OperatorModule())
        );
        return modules;
    }

    @Test
    public void testCreateTableWithAlternativePrimaryKeySyntax() throws Exception {
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement)analyze(
                "create table foo (id integer, name string, primary key (id, name))"
        );

        List<String> primaryKeys = analysis.primaryKeys();
        assertThat(primaryKeys.size(), is(2));
        assertThat(primaryKeys.get(0), is("id"));
        assertThat(primaryKeys.get(1), is("name"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSimpleCreateTable() throws Exception {
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement)analyze(
                "create table foo (id integer primary key, name string) " +
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

        List<String> primaryKeys = analysis.primaryKeys();
        assertThat(primaryKeys.size(), is(1));
        assertThat(primaryKeys.get(0), is("id"));
    }

    @Test
    public void testCreateTableWithRefreshInterval() throws Exception {
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement)analyze(
                "CREATE TABLE foo (id int primary key, content string) " +
                        "with (refresh_interval=5000)");
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.REFRESH_INTERVAL), is("5000"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTableWithRefreshIntervalWrongNumberFormat() throws Exception {
        analyze("CREATE TABLE foo (id int primary key, content string) " +
                "with (refresh_interval='1asdf')");
    }

    @Test
    public void testAlterTableWithRefreshInterval() throws Exception {
        // alter t set
        AlterTableAnalyzedStatement analysisSet = (AlterTableAnalyzedStatement)analyze(
                "ALTER TABLE user_refresh_interval " +
                "SET (refresh_interval = '5000')");
        assertEquals("5000", analysisSet.tableParameter().settings().get(TableParameterInfo.REFRESH_INTERVAL));

        // alter t reset
        AlterTableAnalyzedStatement analysisReset = (AlterTableAnalyzedStatement)analyze(
                "ALTER TABLE user_refresh_interval " +
                "RESET (refresh_interval)");
        assertEquals("1000", analysisReset.tableParameter().settings().get(TableParameterInfo.REFRESH_INTERVAL));
    }

    @Test
    public void testAlterTableWithColumnPolicy() throws Exception {
        AlterTableAnalyzedStatement analysisSet = (AlterTableAnalyzedStatement)analyze(
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
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement)analyze(
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
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement)analyze(
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
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement)analyze(
                "create table foo (id integer primary key, details object(strict) as (name string, age integer))");

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> details = (Map<String, Object>)mappingProperties.get("details");

        assertThat((String)details.get("type"), is("object"));
        assertThat((String) details.get("dynamic"), is("strict"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithIgnoredObject() throws Exception {
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement)analyze(
                "create table foo (id integer primary key, details object(ignored))");

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> details = (Map<String, Object>)mappingProperties.get("details");

        assertThat((String) details.get("type"), is("object"));
        assertThat((String) details.get("dynamic"), is("false"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithSubscriptInFulltextIndexDefinition() throws Exception {
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement)analyze(
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
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement)analyze(
                "create table foo (id integer primary key, details array(string))");
        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> details = (Map<String, Object>)mappingProperties.get("details");

        assertThat((String)details.get("type"), is("array"));
        Map<String, Object> inner = (Map<String, Object>)details.get("inner");
        assertThat((String)inner.get("type"), is("string"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithObjectsArray() throws Exception {
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement)analyze(
                "create table foo (id integer primary key, details array(object as (name string, age integer, tags array(string))))");

        Map<String, Object> metaMapping = (Map) analysis.mapping().get("_meta");
        assertThat(Joiner.on(", ").withKeyValueSeparator(":").join(metaMapping), is("primary_keys:[id]"));

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        assertThat(Joiner.on(", ").withKeyValueSeparator(":").join(mappingProperties), is(
                    "details:{" +
                      "inner={dynamic=true, index=not_analyzed, store=false, properties={" +
                            "tags={inner={index=not_analyzed, store=false, doc_values=false, type=string}, " +
                                "type=array}, " +
                            "age={index=not_analyzed, store=false, doc_values=true, type=integer}, " +
                            "name={index=not_analyzed, store=false, doc_values=true, type=string}}, " +
                          "doc_values=false, "+
                        "type=object}, " +
                      "type=array}, id:{index=not_analyzed, store=false, doc_values=true, type=integer}"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithAnalyzer() throws Exception {
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement)analyze(
                "create table foo (id integer primary key, content string INDEX using fulltext with (analyzer='german'))");

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> contentMapping = (Map<String, Object>)mappingProperties.get("content");

        assertThat((String)contentMapping.get("index"), is("analyzed"));
        assertThat((String) contentMapping.get("analyzer"), is("german"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithAnalyzerParameter() throws Exception {
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement)analyze(
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
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement)analyze(
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

    @Test (expected = IllegalArgumentException.class)
    public void testCreateTableWithSchemaName() throws Exception {
        analyze("create table something.foo (id integer primary key)");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithIndexColumn() throws Exception {
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement)analyze(
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
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement)analyze(
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
                (AlterTableAnalyzedStatement)analyze("alter table users set (number_of_replicas=2)");

        assertThat(analysis.table().ident().name(), is("users"));
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.NUMBER_OF_REPLICAS), is("2"));
    }

    @Test
    public void testResetNumberOfReplicas() throws Exception {
        AlterTableAnalyzedStatement analysis =
                (AlterTableAnalyzedStatement)analyze("alter table users reset (number_of_replicas)");

        assertThat(analysis.table().ident().name(), is("users"));
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.NUMBER_OF_REPLICAS), is("1"));
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.AUTO_EXPAND_REPLICAS), is("false"));
    }

    @Test (expected = IllegalArgumentException.class)
    public void testAlterTableWithInvalidProperty() throws Exception {
        analyze("alter table users set (foobar='2')");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testAlterSystemTable() throws Exception {
        analyze("alter table sys.shards reset (number_of_replicas)");
    }

    @Test
    public void testCreateTableWithMultiplePrimaryKeys() throws Exception {
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement) analyze(
                "create table test (id integer primary key, name string primary key)");

        List<String> primaryKeys = analysis.primaryKeys();
        assertThat(primaryKeys.size(), is(2));
        assertThat(primaryKeys.get(0), is("id"));
        assertThat(primaryKeys.get(1), is("name"));
    }

    @Test
    public void testCreateTableWithMultiplePrimaryKeysAndClusteredBy() throws Exception {
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement) analyze(
                "create table test (id integer primary key, name string primary key) " +
                        "clustered by(name)");

        List<String> primaryKeys = analysis.primaryKeys();
        assertThat(primaryKeys.size(), is(2));
        assertThat(primaryKeys.get(0), is("id"));
        assertThat(primaryKeys.get(1), is("name"));

        Map<String, Object> meta = (Map)analysis.mapping().get("_meta");
        assertNotNull(meta);
        assertThat((String)meta.get("routing"), is("name"));

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
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement) analyze("create table my_table (" +
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
        CreateTableAnalyzedStatement analyze = (CreateTableAnalyzedStatement)analyze(
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
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement) analyze("create table my_table (" +
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
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement) analyze("create table my_table (" +
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
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement)analyze(
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
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement)analyze(
                "create table t (id int primary key) clustered into ? shards", new Object[]{2});
        assertThat(analysis.tableParameter().settings().get(TableParameterInfo.NUMBER_OF_SHARDS), is("2"));
    }

    @Test
    public void testCreateTableWithClusteredIntoShardsParameterNonNumeric() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid number 'foo'");
        analyze("create table t (id int primary key) clustered into ? shards", new Object[]{"foo"});
    }
}
