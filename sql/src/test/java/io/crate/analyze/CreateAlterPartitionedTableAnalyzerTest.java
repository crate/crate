/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.PartitionName;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.information.MetaDataInformationModule;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.operation.operator.OperatorModule;
import io.crate.testing.MockedClusterServiceModule;
import org.apache.lucene.util.BytesRef;
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
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CreateAlterPartitionedTableAnalyzerTest extends BaseAnalyzerTest {

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
            when(schemaInfo.getTableInfo(TEST_PARTITIONED_TABLE_IDENT.name())).thenReturn(TEST_PARTITIONED_TABLE_INFO);
            when(schemaInfo.getTableInfo(TEST_MULTIPLE_PARTITIONED_TABLE_IDENT.name())).thenReturn(TEST_MULTIPLE_PARTITIONED_TABLE_INFO);
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
    public void testPartitionedBy() throws Exception {
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement) analyze("create table my_table (" +
                "  id integer," +
                "  no_index string index off," +
                "  name string," +
                "  date timestamp" +
                ") partitioned by (name)");
        assertThat(analysis.partitionedBy().size(), is(1));
        assertThat(analysis.partitionedBy().get(0), contains("name", "string"));

        // partitioned columns should not appear as regular columns
        assertThat(analysis.mappingProperties(), not(hasKey("name")));
        Map<String, Object> metaMapping = (Map) analysis.mapping().get("_meta");
        assertThat((Map<String, Object>) metaMapping.get("columns"), not(hasKey("name")));
        List<List<String>> partitionedByMeta = (List<List<String>>)metaMapping.get("partitioned_by");
        assertTrue(analysis.isPartitioned());
        assertThat(partitionedByMeta.size(), is(1));
        assertThat(partitionedByMeta.get(0).get(0), is("name"));
        assertThat(partitionedByMeta.get(0).get(1), is("string"));
    }

    @Test
    public void testPartitionedByMultipleColumns() throws Exception {
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement) analyze("create table my_table (" +
                "  id integer," +
                "  no_index string index off," +
                "  name string," +
                "  date timestamp" +
                ") partitioned by (name, date)");
        assertThat(analysis.partitionedBy().size(), is(2));
        assertThat(analysis.mappingProperties(), allOf(
                not(hasKey("name")),
                not(hasKey("date"))
        ));
        assertThat((Map<String, Object>) ((Map) analysis.mapping().get("_meta")).get("columns"),
                allOf(
                        not(hasKey("name")),
                        not(hasKey("date"))
                ));
        assertThat(analysis.partitionedBy().get(0), contains("name", "string"));
        assertThat(analysis.partitionedBy().get(1), contains("date", "date"));
    }

    @Test
    public void testPartitionedByNestedColumns() throws Exception {
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement) analyze("create table my_table (" +
                "  id integer," +
                "  no_index string index off," +
                "  o object as (" +
                "    name string" +
                "  )," +
                "  date timestamp" +
                ") partitioned by (date, o['name'])");
        assertThat(analysis.partitionedBy().size(), is(2));
        assertThat(analysis.mappingProperties(), not(hasKey("name")));
        assertThat((Map<String, Object>) ((Map) analysis.mappingProperties().get("o")).get("properties"), not(hasKey("name")));
        assertThat((Map<String, Object>) ((Map) analysis.mapping().get("_meta")).get("columns"), not(hasKey("date")));

        Map metaColumns = (Map) ((Map) analysis.mapping().get("_meta")).get("columns");
        assertNull(metaColumns);
        assertThat(analysis.partitionedBy().get(0), contains("date", "date"));
        assertThat(analysis.partitionedBy().get(1), contains("o.name", "string"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionedByArrayNestedColumns() throws Exception {
        analyze("create table my_table (" +
                "  a array(object as (" +
                "    name string" +
                "  ))," +
                "  date timestamp" +
                ") partitioned by (date, a['name'])");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionedByArray() throws Exception {
        analyze("create table my_table (" +
                "  a array(string)," +
                "  date timestamp" +
                ") partitioned by (a)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionedByInnerArray() throws Exception {
        analyze("create table my_table (" +
                "  a object as (names array(string))," +
                "  date timestamp" +
                ") partitioned by (a['names'])");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionedByObject() throws Exception {
        analyze("create table my_table (" +
                "  a object as(name string)," +
                "  date timestamp" +
                ") partitioned by (a)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionedByInnerObject() throws Exception {
        analyze("create table my_table (" +
                "  a object as(b object as(name string))," +
                "  date timestamp" +
                ") partitioned by (a['b'])");
    }

    @Test
    public void testPartitionByUnknownColumn() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        analyze("create table my_table (p string) partitioned by (a)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionedByNotPartOfPrimaryKey() throws Exception {
        analyze("create table my_table (" +
                "  id1 integer," +
                "  id2 integer," +
                "  date timestamp," +
                "  primary key (id1, id2)" +
                ") partitioned by (id1, date)");
    }

    @Test
    public void testPartitionedByPartOfPrimaryKey() throws Exception {
        CreateTableAnalyzedStatement analysis = (CreateTableAnalyzedStatement) analyze("create table my_table (" +
                "  id1 integer," +
                "  id2 integer," +
                "  date timestamp," +
                "  primary key (id1, id2)" +
                ") partitioned by (id1)");
        assertThat(analysis.partitionedBy().size(), is(1));
        assertThat(analysis.partitionedBy().get(0), contains("id1", "integer"));
        assertThat(analysis.mappingProperties(), not(hasKey("id1")));
        assertThat((Map<String, Object>) ((Map) analysis.mapping().get("_meta")).get("columns"),
                not(hasKey("id1")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionedByIndexed() throws Exception {
        analyze("create table my_table(" +
                "  name string index using fulltext," +
                "  no_index string index off," +
                "  stuff string," +
                "  o object as (s string)," +
                "  index ft using fulltext(stuff, o['s']) with (analyzer='snowball')" +
                ") partitioned by (name)");

    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionedByCompoundIndex() throws Exception {
        analyze("create table my_table(" +
                "  name string index using fulltext," +
                "  no_index string index off," +
                "  stuff string," +
                "  o object as (s string)," +
                "  index ft using fulltext(stuff, o['s']) with (analyzer='snowball')" +
                ") partitioned by (ft)");

    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionedByClusteredBy() throws Exception {
        analyze("create table my_table (" +
                "  id integer," +
                "  name string" +
                ") partitioned by (id)" +
                "  clustered by (id) into 5 shards");
    }

    @Test
    public void testAlterPartitionedTable() throws Exception {
        AlterTableAnalyzedStatement analysis = (AlterTableAnalyzedStatement)analyze(
                "alter table parted set (number_of_replicas='0-all')");
        assertThat(analysis.partitionName().isPresent(), is(false));
        assertThat(analysis.table().isPartitioned(), is(true));
        assertEquals("0-all", analysis.tableParameter().settings().get(TableParameterInfo.AUTO_EXPAND_REPLICAS));
    }

    @Test
    public void testAlterPartitionedTablePartition() throws Exception {
        AlterTableAnalyzedStatement analysis = (AlterTableAnalyzedStatement) analyze(
                "alter table parted partition (date=1395874800000) set (number_of_replicas='0-all')");
        assertThat(analysis.partitionName().isPresent(), is(true));
        assertThat(analysis.partitionName().get(), is(new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000")))));
        assertThat(analysis.table().tableParameterInfo(), instanceOf(AlterPartitionedTableParameterInfo.class));
        AlterPartitionedTableParameterInfo tableSettingsInfo = (AlterPartitionedTableParameterInfo)analysis.table().tableParameterInfo();
        assertThat(tableSettingsInfo.partitionTableSettingsInfo(), instanceOf(TableParameterInfo.class));
        assertEquals("0-all", analysis.tableParameter().settings().get(TableParameterInfo.AUTO_EXPAND_REPLICAS));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAlterPartitionedTableNonExistentPartition() throws Exception {
        analyze("alter table parted partition (date='1970-01-01') set (number_of_replicas='0-all')");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAlterPartitionedTableInvalidPartitionColumns() throws Exception {
        analyze("alter table parted partition (a=1) set (number_of_replicas='0-all')");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAlterPartitionedTableInvalidNumber() throws Exception {
        analyze("alter table multi_parted partition (date=1395874800000) set (number_of_replicas='0-all')");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAlterTableWithPartitionClause() throws Exception {
        analyze("alter table users partition (date='1970-01-01') reset (number_of_replicas)");
    }

    @Test
    public void testAlterPartitionedTableShards() throws Exception {
        AlterTableAnalyzedStatement analysis = (AlterTableAnalyzedStatement)analyze(
                "alter table parted set (number_of_shards=10)");
        assertThat(analysis.partitionName().isPresent(), is(false));
        assertThat(analysis.table().isPartitioned(), is(true));
        assertThat(analysis.table().tableParameterInfo(), instanceOf(AlterPartitionedTableParameterInfo.class));
        assertEquals("10", analysis.tableParameter().settings().get(TableParameterInfo.NUMBER_OF_SHARDS));

        AlterPartitionedTableParameterInfo tableSettingsInfo = (AlterPartitionedTableParameterInfo)analysis.table().tableParameterInfo();
        TableParameter tableParameter = new TableParameter(
                analysis.tableParameter().settings(),
                tableSettingsInfo.partitionTableSettingsInfo().supportedInternalSettings());
        assertEquals(null, tableParameter.settings().get(TableParameterInfo.NUMBER_OF_SHARDS));

    }

    @Test
    public void testAlterPartitionedTablePartitionColumnPolicy() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid property \"column_policy\" passed to [ALTER | CREATE] TABLE statement");
        analyze("alter table parted partition (date=1395874800000) set (column_policy='strict')");
    }

}
