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

package io.crate.integrationtests;

import static io.crate.metadata.table.ColumnPolicies.decodeMappingValue;
import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_COLUMN;
import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.ParsedXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.IntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.Constants;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.testing.TestingHelpers;

@IntegTestCase.ClusterScope(numDataNodes = 1)
public class ColumnPolicyIntegrationTest extends IntegTestCase {

    private String copyFilePath = Paths.get(getClass().getResource("/essetup/data/copy").toURI()).toUri().toString();

    public ColumnPolicyIntegrationTest() throws URISyntaxException {
    }

    private MappingMetadata getMappingMetadata(String index) {
        return clusterService().state().metadata().indices()
            .get(index)
            .mapping();
    }

    private Map<String, Object> getSourceMap(String index) throws IOException {
        return getMappingMetadata(getFqn(index)).sourceAsMap();
    }

    private Map<String, Object> getSourceMap(String schema, String index) throws IOException {
        return getMappingMetadata(getFqn(schema, index)).sourceAsMap();
    }

    @Test
    public void testInsertNewColumnTableStrictColumnPolicy() throws Exception {
        execute("create table strict_table (" +
                "  id integer primary key, " +
                "  name string" +
                ") with (column_policy='strict', number_of_replicas=0)");
        ensureYellow();
        execute("insert into strict_table (id, name) values (1, 'Ford')");
        execute("refresh table strict_table");

        execute("select * from strict_table");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), is(arrayContaining("id", "name")));
        assertThat(response.rows()[0], is(Matchers.<Object>arrayContaining(1, "Ford")));

        assertThrowsMatches(() -> execute("insert into strict_table (id, name, boo) values (2, 'Trillian', true)"),
                     isSQLError(is("Column boo unknown"),
                                UNDEFINED_COLUMN,
                                NOT_FOUND,
                                4043));
    }

    @Test
    public void testUpdateNewColumnTableStrictColumnPolicy() throws Exception {
        execute("create table strict_table (" +
                "  id integer primary key, " +
                "  name string" +
                ") with (column_policy='strict', number_of_replicas=0)");
        ensureYellow();
        execute("insert into strict_table (id, name) values (1, 'Ford')");
        execute("refresh table strict_table");

        execute("select * from strict_table");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), is(arrayContaining("id", "name")));
        assertThat(response.rows()[0], is(Matchers.<Object>arrayContaining(1, "Ford")));

        assertThrowsMatches(() -> execute("update strict_table set name='Trillian', boo=true where id=1"),
                     isSQLError(is("Column boo unknown"), UNDEFINED_COLUMN, NOT_FOUND,4043));
    }

    @Test
    public void testCopyFromFileStrictTable() throws Exception {
        execute("create table quotes (id int primary key) with (column_policy='strict', number_of_replicas = 0)");
        ensureYellow();

        execute("copy quotes from ?", new Object[]{copyFilePath + "test_copy_from.json"});
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testInsertNewColumnTableDynamic() throws Exception {
        execute("create table dynamic_table (" +
                "  id integer primary key, " +
                "  name string" +
                ") with (column_policy='dynamic', number_of_replicas=0)");
        ensureYellow();
        execute("insert into dynamic_table (id, name) values (1, 'Ford')");
        execute("refresh table dynamic_table");

        execute("select * from dynamic_table");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), is(arrayContaining("id", "name")));
        assertThat(response.rows()[0], is(Matchers.<Object>arrayContaining(1, "Ford")));

        execute("insert into dynamic_table (id, name, boo) values (2, 'Trillian', true)");
        execute("refresh table dynamic_table");

        waitForMappingUpdateOnAll("dynamic_table", "boo");
        execute("select * from dynamic_table order by id");
        assertThat(response.rowCount(), is(2L));
        assertThat(response.cols().length, is(3));
        assertThat(response.cols(), is(arrayContaining("id", "name", "boo")));
        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "1| Ford| NULL\n" +
            "2| Trillian| true\n"));
    }

    @Test
    public void testInsertArrayIntoDynamicTable() throws Exception {
        execute("create table dynamic_table (" +
                "  meta string" +
                ") with (column_policy='dynamic', number_of_replicas=0)");
        ensureYellow();
        execute("insert into dynamic_table (new, meta) values(['a', 'b', 'c'], 'hello')");
        execute("insert into dynamic_table (new) values(['d', 'e', 'f'])");
        waitForMappingUpdateOnAll("dynamic_table", "new", "meta");
        Map<String, Object> sourceMap = getSourceMap("dynamic_table");
        assertThat(String.valueOf(nestedValue(sourceMap, "properties.new.type")), is("array"));
        assertThat(String.valueOf(nestedValue(sourceMap, "properties.new.inner.type")), is("keyword"));
        assertThat(String.valueOf(nestedValue(sourceMap, "properties.meta.type")), is("keyword"));
    }

    @Test
    public void testInsertDynamicObjectArray() throws Exception {
        execute("create table dynamic_table (person object(dynamic)) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into dynamic_table (person) values " +
                "({name='Ford', addresses=[{city='West Country', country='GB'}]})");
        refresh();
        waitForMappingUpdateOnAll("dynamic_table", "person.name");

        Map<String, Object> sourceMap = getSourceMap("dynamic_table");
        assertThat(String.valueOf(nestedValue(sourceMap, "properties.person.properties.addresses.type")), is("array"));
        assertThat(String.valueOf(nestedValue(sourceMap, "properties.person.properties.name.type")), is("keyword"));
        assertThat(String.valueOf(nestedValue(sourceMap, "properties.person.properties.addresses.inner.properties.city.type")), is("keyword"));
        assertThat(String.valueOf(nestedValue(sourceMap, "properties.person.properties.addresses.inner.properties.country.type")), is("keyword"));

        execute("select person['name'], person['addresses']['city'] from dynamic_table ");
        assertEquals(1L, response.rowCount());
        assertArrayEquals(new String[]{"person['name']", "person['addresses']['city']"},
            response.cols());

        assertThat(response.rows()[0][0], is("Ford"));
        assertThat((List<Object>) response.rows()[0][1], Matchers.contains("West Country"));
    }

    @Test
    public void testInsertNestedArrayIntoDynamicTable() throws Exception {
        execute("create table dynamic_table (" +
                "  meta string" +
                ") with (column_policy='dynamic', number_of_replicas=0)");
        ensureYellow();
        execute("insert into dynamic_table (new, meta) values({a=['a', 'b', 'c'], nest={a=['a','b']}}, 'hello')");
        execute("refresh table dynamic_table");
        execute("insert into dynamic_table (new) values({a=['d', 'e', 'f']})");
        execute("refresh table dynamic_table");
        execute("insert into dynamic_table (new) values({nest={}, new={}})");

        waitForMappingUpdateOnAll("dynamic_table", "new");
        Map<String, Object> sourceMap = getSourceMap("dynamic_table");
        assertThat(String.valueOf(nestedValue(sourceMap, "properties.new.properties.a.type")), is("array"));
        assertThat(String.valueOf(nestedValue(sourceMap, "properties.new.properties.a.inner.type")), is("keyword"));
        assertThat(String.valueOf(nestedValue(sourceMap, "properties.new.properties.nest.properties.a.type")), is("array"));
        assertThat(String.valueOf(nestedValue(sourceMap, "properties.new.properties.nest.properties.a.inner.type")), is("keyword"));
        assertThat(String.valueOf(nestedValue(sourceMap, "properties.meta.type")), is("keyword"));
    }

    @Test
    public void testInsertNestedObjectOnCustomSchemaTable() throws Exception {
        execute("create table c.dynamic_table (" +
                "  meta object(strict) as (" +
                "     meta object" +
                "  )" +
                ") with (column_policy='dynamic', number_of_replicas=0)");
        ensureYellow();
        execute("insert into c.dynamic_table (meta) values({meta={a=['a','b']}})");
        execute("refresh table c.dynamic_table");
        execute("insert into c.dynamic_table (meta) values({meta={a=['c','d']}})");
        waitForMappingUpdateOnAll(new RelationName("c", "dynamic_table"), "meta.meta.a");
        Map<String, Object> sourceMap = getSourceMap("c", "dynamic_table");
        assertThat(String.valueOf(nestedValue(sourceMap, "properties.meta.properties.meta.properties.a.type")), is("array"));
        assertThat(String.valueOf(nestedValue(sourceMap, "properties.meta.properties.meta.properties.a.inner.type")), is("keyword"));
    }

    @Test
    public void testInsertMultipleValuesDynamic() throws Exception {
        execute("create table dynamic_table (" +
                "  my_object object " +
                ") with (column_policy='dynamic', number_of_replicas=0)");
        ensureYellow();

        execute("insert into dynamic_table (my_object) values ({a=['a','b']}),({b=['a']})");
        execute("refresh table dynamic_table");

        waitForMappingUpdateOnAll("dynamic_table", "my_object.a", "my_object.b");
        Map<String, Object> sourceMap = getSourceMap("dynamic_table");
        assertThat(String.valueOf(nestedValue(sourceMap, "properties.my_object.properties.a.type")), is("array"));
        assertThat(String.valueOf(nestedValue(sourceMap, "properties.my_object.properties.a.inner.type")), is("keyword"));
        assertThat(String.valueOf(nestedValue(sourceMap, "properties.my_object.properties.b.type")), is("array"));
        assertThat(String.valueOf(nestedValue(sourceMap, "properties.my_object.properties.b.inner.type")), is("keyword"));
    }

    @Test
    public void testAddColumnToStrictObject() throws Exception {
        execute("create table books(" +
                "   author object(dynamic) as (" +
                "       name object(strict) as (" +
                "           first_name string" +
                "       )" +
                "   )," +
                "   title string" +
                ")");
        ensureYellow();
        Map<String, Object> authorMap = Map.of("name", Map.of("first_name", "Douglas"));
        execute("insert into books (title, author) values (?,?)",
            new Object[]{
                "The Hitchhiker's Guide to the Galaxy",
                authorMap
            });
        execute("refresh table books");
        assertThrowsMatches(() -> execute("insert into books (title, author) values (?,?)",
            new Object[]{
                "Life, the Universe and Everything",
                Map.of("name", Map.of("first_name", "Douglas", "middle_name", "Noel"))
            }), isSQLError(containsString("dynamic introduction of [middle_name] within [author.name] is not allowed"),
                           INTERNAL_ERROR,
                           BAD_REQUEST,
                           4000));
    }

    public Object nestedValue(Map<String, Object> map, String dottedPath) {
        String[] paths = dottedPath.split("\\.");
        Object value = null;
        for (String key : paths) {
            value = map.get(key);
            if (value instanceof Map) {
                map = (Map<String, Object>) map.get(key);
            }
        }
        return value;
    }

    @Test
    public void testUpdateNewColumnTableDynamic() throws Exception {
        execute("create table dynamic_table (" +
                "  id integer primary key, " +
                "  name string" +
                ") with (column_policy='dynamic', number_of_replicas=0)");
        ensureYellow();
        execute("insert into dynamic_table (id, name) values (1, 'Ford')");
        execute("refresh table dynamic_table");

        execute("select * from dynamic_table");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), is(arrayContaining("id", "name")));
        assertThat(response.rows()[0], is(Matchers.<Object>arrayContaining(1, "Ford")));

        execute("update dynamic_table set name='Trillian', boo=true where name='Ford'");
        execute("refresh table dynamic_table");

        waitForMappingUpdateOnAll("dynamic_table", "boo");
        execute("select * from dynamic_table");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), is(arrayContaining("id", "name", "boo")));
        assertThat(response.rows()[0], is(Matchers.arrayContaining(1, "Trillian", true)));
    }

    @Test
    public void testInsertNewColumnTableDefault() throws Exception {
        execute("create table dynamic_table (" +
                "  id integer primary key, " +
                "  score double" +
                ") with (number_of_replicas=0, column_policy='dynamic')");
        ensureYellow();
        execute("insert into dynamic_table (id, score) values (1, 42.24)");
        execute("refresh table dynamic_table");

        execute("select * from dynamic_table");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), is(arrayContaining("id", "score")));
        assertThat(response.rows()[0], is(Matchers.<Object>arrayContaining(1, 42.24D)));

        execute("insert into dynamic_table (id, score, good) values (2, -0.01, false)");
        execute("refresh table dynamic_table");

        waitForMappingUpdateOnAll("dynamic_table", "good");
        execute("select * from dynamic_table order by id");
        assertThat(response.rowCount(), is(2L));
        assertThat(response.cols(), is(arrayContaining("id", "score", "good")));
        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "1| 42.24| NULL\n" +
            "2| -0.01| false\n"));
    }

    @Test
    public void testUpdateNewColumnTableDefault() throws Exception {
        execute("create table dynamic_table (" +
                "  id integer primary key, " +
                "  score double" +
                ") with (number_of_replicas=0, column_policy='dynamic')");
        ensureYellow();
        execute("insert into dynamic_table (id, score) values (1, 4656234.345)");
        execute("refresh table dynamic_table");

        execute("select * from dynamic_table");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), is(arrayContaining("id", "score")));
        assertThat(response.rows()[0], is(Matchers.arrayContaining(1, 4656234.345D)));

        execute("update dynamic_table set name='Trillian', good=true where score > 0.0");
        execute("refresh table dynamic_table");

        waitForMappingUpdateOnAll("dynamic_table", "name");
        execute("select * from dynamic_table");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), is(arrayContaining("id", "score", "name", "good")));
        assertThat(response.rows()[0], is(Matchers.arrayContaining(1, 4656234.345D, "Trillian", true)));
    }

    @Test
    public void testStrictPartitionedTableInsert() throws Exception {
        execute("create table numbers (" +
                "  num int, " +
                "  odd boolean," +
                "  prime boolean" +
                ") partitioned by (odd) with (column_policy='strict', number_of_replicas=0)");
        ensureYellow();

        GetIndexTemplatesResponse response = client().admin().indices()
            .getTemplates(new GetIndexTemplatesRequest(PartitionName.templateName(sqlExecutor.getCurrentSchema(), "numbers")))
            .get();
        assertThat(response.getIndexTemplates().size(), is(1));
        IndexTemplateMetadata template = response.getIndexTemplates().get(0);
        CompressedXContent mappingStr = template.mappings().get(Constants.DEFAULT_MAPPING_TYPE);
        assertThat(mappingStr, is(notNullValue()));
        ParsedXContent typeAndMap =
            XContentHelper.convertToMap(mappingStr.compressedReference(), false, XContentType.JSON);
        @SuppressWarnings("unchecked")
        Map<String, Object> mapping = (Map<String, Object>) typeAndMap.map().get(Constants.DEFAULT_MAPPING_TYPE);
        assertThat(decodeMappingValue(mapping.get("dynamic")), is(ColumnPolicy.STRICT));

        execute("insert into numbers (num, odd, prime) values (?, ?, ?)",
            new Object[]{6, true, false});
        execute("refresh table numbers");

        Map<String, Object> sourceMap = getSourceMap(
            new PartitionName(new RelationName("doc", "numbers"), Arrays.asList("true")).asIndexName());
        assertThat(decodeMappingValue(sourceMap.get("dynamic")), is(ColumnPolicy.STRICT));

        assertThrowsMatches(() -> execute("insert into numbers (num, odd, prime, perfect) values (?, ?, ?, ?)",
                                   new Object[]{28, true, false, true}),
                     isSQLError(is("Column perfect unknown"),
                                UNDEFINED_COLUMN,
                                NOT_FOUND,
                                4043
                     ));
    }

    @Test
    public void testStrictPartitionedTableUpdate() throws Exception {
        execute("create table numbers (" +
                "  num int, " +
                "  odd boolean," +
                "  prime boolean" +
                ") partitioned by (odd) with (column_policy='strict', number_of_replicas=0)");
        ensureYellow();

        GetIndexTemplatesResponse response = client().admin().indices()
            .getTemplates(new GetIndexTemplatesRequest(PartitionName.templateName(sqlExecutor.getCurrentSchema(), "numbers")))
            .get();
        assertThat(response.getIndexTemplates().size(), is(1));
        IndexTemplateMetadata template = response.getIndexTemplates().get(0);
        CompressedXContent mappingStr = template.mappings().get(Constants.DEFAULT_MAPPING_TYPE);
        assertThat(mappingStr, is(notNullValue()));
        ParsedXContent typeAndMap = XContentHelper.convertToMap(mappingStr.compressedReference(), false);
        @SuppressWarnings("unchecked")
        Map<String, Object> mapping = (Map<String, Object>) typeAndMap.map().get(Constants.DEFAULT_MAPPING_TYPE);
        assertThat(decodeMappingValue(mapping.get("dynamic")), is(ColumnPolicy.STRICT));

        execute("insert into numbers (num, odd, prime) values (?, ?, ?)",
            new Object[]{6, true, false});
        execute("refresh table numbers");

        Map<String, Object> sourceMap = getSourceMap(
            new PartitionName(new RelationName("doc", "numbers"), Arrays.asList("true")).asIndexName());
        assertThat(decodeMappingValue(sourceMap.get("dynamic")), is(ColumnPolicy.STRICT));

        assertThrowsMatches(() -> execute("update numbers set num=?, perfect=? where num=6",
                                   new Object[]{28, true}),
                     isSQLError(is("Column perfect unknown"),
                                UNDEFINED_COLUMN,
                                NOT_FOUND,
                                4043));
    }

    @Test
    public void testDynamicPartitionedTable() throws Exception {
        execute("create table numbers (" +
                "  num int, " +
                "  odd boolean," +
                "  prime boolean" +
                ") partitioned by (odd) with (column_policy='dynamic', number_of_replicas=0)");
        ensureYellow();

        GetIndexTemplatesResponse templateResponse = client().admin().indices()
            .getTemplates(new GetIndexTemplatesRequest(PartitionName.templateName(sqlExecutor.getCurrentSchema(), "numbers")))
            .get();
        assertThat(templateResponse.getIndexTemplates().size(), is(1));
        IndexTemplateMetadata template = templateResponse.getIndexTemplates().get(0);
        CompressedXContent mappingStr = template.mappings().get(Constants.DEFAULT_MAPPING_TYPE);
        assertThat(mappingStr, is(notNullValue()));
        ParsedXContent typeAndMap = XContentHelper.convertToMap(mappingStr.compressedReference(), false);
        @SuppressWarnings("unchecked")
        Map<String, Object> mapping = (Map<String, Object>) typeAndMap.map().get(Constants.DEFAULT_MAPPING_TYPE);
        assertThat(String.valueOf(mapping.get("dynamic")), is("true"));

        execute("insert into numbers (num, odd, prime) values (?, ?, ?)",
            new Object[]{6, true, false});
        execute("refresh table numbers");

        Map<String, Object> sourceMap = getSourceMap(
            new PartitionName(new RelationName("doc", "numbers"), Collections.singletonList("true")).asIndexName());
        assertThat(String.valueOf(sourceMap.get("dynamic")), is("true"));

        execute("insert into numbers (num, odd, prime, perfect) values (?, ?, ?, ?)",
            new Object[]{28, true, false, true});
        ensureYellow();
        execute("refresh table numbers");

        waitForMappingUpdateOnAll("numbers", "perfect");
        execute("select * from numbers order by num");
        assertThat(response.rowCount(), is(2L));
        assertThat(response.cols(), arrayContaining("num", "odd", "prime", "perfect"));
        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "6| true| false| NULL\n" +
            "28| true| false| true\n"));

        execute("update numbers set prime=true, changed='2014-10-23T10:20', author='troll' where num=28");
        assertThat(response.rowCount(), is(1L));

        waitForMappingUpdateOnAll("numbers", "changed");
    }

    @Test
    public void testAlterDynamicTable() throws Exception {
        execute("create table dynamic_table (" +
                "  id integer primary key, " +
                "  score double" +
                ") with (number_of_replicas=0)");
        ensureYellow();
        execute("alter table dynamic_table set (column_policy = 'strict')");
        waitNoPendingTasksOnAll();
        assertThrowsMatches(() -> execute("insert into dynamic_table (id, score, new_col) values (1, 4656234.345, 'hello')"),
                     isSQLError(is("Column new_col unknown"), UNDEFINED_COLUMN, NOT_FOUND,4043));
    }

    @Test
    public void testAlterTable() throws Exception {
        execute("create table dynamic_table (" +
                "  id integer primary key, " +
                "  score double" +
                ") with (number_of_replicas=0, column_policy='strict')");
        ensureYellow();
        execute("insert into dynamic_table (id, score) values (1, 42)");
        execute("alter table dynamic_table set (column_policy = 'dynamic')");
        waitNoPendingTasksOnAll();
        execute("insert into dynamic_table (id, score, new_col) values (2, 4656234.345, 'hello')");
    }

    @Test
    public void testResetColumnPolicy() throws Exception {
        execute("create table dynamic_table (" +
                "  id integer, " +
                "  score double" +
                ") with (number_of_replicas=0)");
        ensureYellow();
        execute("alter table dynamic_table set (column_policy = 'dynamic')");
        waitNoPendingTasksOnAll();
        assertThat(
            decodeMappingValue(getSourceMap("dynamic_table").get("dynamic")),
            is(ColumnPolicy.DYNAMIC));
        execute("alter table dynamic_table reset (column_policy)");
        waitNoPendingTasksOnAll();
        assertThat(
            decodeMappingValue(getSourceMap("dynamic_table").get("dynamic")),
            is(ColumnPolicy.STRICT));
    }

    @Test
    public void testResetColumnPolicyPartitioned() throws Exception {
        execute("create table dynamic_table (" +
                "  id integer, " +
                "  score double" +
                ") partitioned by (score) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into dynamic_table (id, score) values (1, 10)");
        execute("refresh table dynamic_table");
        execute("alter table dynamic_table set (column_policy = 'dynamic')");
        waitNoPendingTasksOnAll();
        String indexName = new PartitionName(
            new RelationName("doc", "dynamic_table"), Arrays.asList("10.0")).asIndexName();
        Map<String, Object> sourceMap = getSourceMap(indexName);
        assertThat(decodeMappingValue(sourceMap.get("dynamic")), is(ColumnPolicy.DYNAMIC));
        execute("alter table dynamic_table reset (column_policy)");
        waitNoPendingTasksOnAll();
        sourceMap = getSourceMap(indexName);
        assertThat(decodeMappingValue(sourceMap.get("dynamic")), is(ColumnPolicy.STRICT));
    }

    @Test
    public void testAlterColumnPolicyOnPartitionedTableWithExistingPartitions() throws Exception {
        execute("create table dynamic_table (" +
                "  id integer, " +
                "  score double" +
                ") partitioned by (score) with (number_of_replicas=0, column_policy='strict')");
        ensureYellow();
        // create at least 2 partitions to test real multi partition logic (1 partition would behave similar to 1 normal table)
        execute("insert into dynamic_table (id, score) values (1, 10)");
        execute("insert into dynamic_table (id, score) values (1, 20)");
        execute("refresh table dynamic_table");
        ensureYellow();
        execute("alter table dynamic_table set (column_policy = 'dynamic')");
        waitNoPendingTasksOnAll();
        // After changing the column_policy it's possible to add new columns to existing and new
        // partitions
        execute("insert into dynamic_table (id, score, comment) values (2, 10, 'this is a new column')");
        execute("insert into dynamic_table (id, score, new_comment) values (2, 5, 'this is a new column on a new partition')");
        execute("refresh table dynamic_table");
        ensureYellow();
        GetIndexTemplatesResponse response = client().admin().indices()
            .getTemplates(new GetIndexTemplatesRequest(PartitionName.templateName(sqlExecutor.getCurrentSchema(), "dynamic_table")))
            .get();
        assertThat(response.getIndexTemplates().size(), is(1));
        IndexTemplateMetadata template = response.getIndexTemplates().get(0);
        CompressedXContent mappingStr = template.mappings().get(Constants.DEFAULT_MAPPING_TYPE);
        assertThat(mappingStr, is(notNullValue()));
        ParsedXContent typeAndMap =
            XContentHelper.convertToMap(mappingStr.compressedReference(), false, XContentType.JSON);
        @SuppressWarnings("unchecked")
        Map<String, Object> mapping = (Map<String, Object>) typeAndMap.map().get(Constants.DEFAULT_MAPPING_TYPE);
        assertThat(decodeMappingValue(mapping.get("dynamic")), is(ColumnPolicy.DYNAMIC));

        execute("insert into dynamic_table (id, score, new_col) values (?, ?, ?)",
            new Object[]{6, 3, "hello"});
        execute("refresh table dynamic_table");
        ensureYellow();

        Map<String, Object> sourceMap = getSourceMap(
            new PartitionName(new RelationName("doc", "dynamic_table"), Arrays.asList("10.0")).asIndexName());
        assertThat(decodeMappingValue(sourceMap.get("dynamic")), is(ColumnPolicy.DYNAMIC));

        sourceMap = getSourceMap(new PartitionName(
            new RelationName("doc", "dynamic_table"), Arrays.asList("5.0")).asIndexName());
        assertThat(decodeMappingValue(sourceMap.get("dynamic")), is(ColumnPolicy.DYNAMIC));

        sourceMap = getSourceMap(new PartitionName(
            new RelationName("doc", "dynamic_table"), Arrays.asList("3.0")).asIndexName());
        assertThat(decodeMappingValue(sourceMap.get("dynamic")), is(ColumnPolicy.DYNAMIC));
    }

}
