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

import static io.crate.common.collections.Maps.getByPath;
import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_COLUMN;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.Constants;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.planner.optimizer.rule.MergeFilterAndCollect;
import io.crate.planner.optimizer.rule.OptimizeCollectWhereClauseAccess;
import io.crate.server.xcontent.ParsedXContent;
import io.crate.server.xcontent.XContentHelper;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.testing.Asserts;
import io.crate.testing.UseRandomizedOptimizerRules;

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
        assertThat(response).hasColumns("id", "name");
        assertThat(response).hasRows("1| Ford");

        Asserts.assertSQLError(() -> execute("insert into strict_table (id, name, boo) values (2, 'Trillian', true)"))
            .hasPGError(UNDEFINED_COLUMN)
            .hasHTTPError(NOT_FOUND, 4043)
            .hasMessageContaining("Column boo unknown");
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
        assertThat(response).hasColumns("id", "name");
        assertThat(response).hasRows("1| Ford");

        Asserts.assertSQLError(() -> execute("update strict_table set name='Trillian', boo=true where id=1"))
            .hasPGError(UNDEFINED_COLUMN)
            .hasHTTPError(NOT_FOUND, 4043)
            .hasMessageContaining("Column boo unknown");
    }

    @Test
    public void testCopyFromFileStrictTable() throws Exception {
        execute("create table quotes (id int primary key) with (column_policy='strict', number_of_replicas = 0)");
        ensureYellow();

        execute("copy quotes from ?", new Object[]{copyFilePath + "test_copy_from.json"});
        assertThat(response).hasRowCount(0);
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
        assertThat(response).hasColumns("id", "name");
        assertThat(response).hasRows("1| Ford");

        execute("insert into dynamic_table (id, name, boo) values (2, 'Trillian', true)");
        execute("refresh table dynamic_table");

        execute("select * from dynamic_table order by id");
        assertThat(response).hasColumns("id", "name", "boo");
        assertThat(response).hasRows(
            "1| Ford| NULL",
            "2| Trillian| true"
        );
    }

    @Test
    public void testInsertArrayIntoDynamicTable() throws Exception {
        execute("create table dynamic_table (" +
                "  meta string" +
                ") with (column_policy='dynamic', number_of_replicas=0)");
        ensureYellow();
        execute("insert into dynamic_table (new, meta) values(['a', 'b', 'c'], 'hello')");
        execute("insert into dynamic_table (new) values(['d', 'e', 'f'])");
        Map<String, Object> sourceMap = getSourceMap("dynamic_table");
        assertThat(getByPath(sourceMap, "properties.new.type")).isEqualTo("array");
        assertThat(getByPath(sourceMap, "properties.new.inner.type")).isEqualTo("keyword");
        assertThat(getByPath(sourceMap, "properties.meta.type")).isEqualTo("keyword");
    }

    @Test
    // Ensure that PKLookup is hit.
    @UseRandomizedOptimizerRules(alwaysKeep = {MergeFilterAndCollect.class, OptimizeCollectWhereClauseAccess.class})
    public void testInsertDynamicObjectArray() throws Exception {
        execute("create table dynamic_table (id int PRIMARY KEY, person object(dynamic)) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into dynamic_table (id, person) values " +
                "(1, {name='Ford', addresses=[{city='West Country', country='GB'}]})");
        refresh();

        Map<String, Object> sourceMap = getSourceMap("dynamic_table");
        assertThat(getByPath(sourceMap, "properties.person.properties.addresses.type")).isEqualTo("array");
        assertThat(getByPath(sourceMap, "properties.person.properties.name.type")).isEqualTo("keyword");
        assertThat(getByPath(sourceMap, "properties.person.properties.addresses.inner.properties.city.type")).isEqualTo("keyword");
        assertThat(getByPath(sourceMap, "properties.person.properties.addresses.inner.properties.country.type")).isEqualTo("keyword");

        execute("select person['name'], person['addresses']['city'] from dynamic_table");

        assertThat(response).hasColumns("person['name']", "person['addresses']['city']");
        assertThat(response).hasRows(
            "Ford| [West Country]"
        );

        // Verify that PKLookup can fetch a sub-column, which is array of objects.
        execute("select person['addresses']['city'] from dynamic_table where id = 1");
        assertThat(response).hasRows("[West Country]");
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

        Map<String, Object> sourceMap = getSourceMap("dynamic_table");
        assertThat(getByPath(sourceMap, "properties.new.properties.a.type")).isEqualTo("array");
        assertThat(getByPath(sourceMap, "properties.new.properties.a.inner.type")).isEqualTo("keyword");
        assertThat(getByPath(sourceMap, "properties.new.properties.nest.properties.a.type")).isEqualTo("array");
        assertThat(getByPath(sourceMap, "properties.new.properties.nest.properties.a.inner.type")).isEqualTo("keyword");
        assertThat(getByPath(sourceMap, "properties.meta.type")).isEqualTo("keyword");
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
        Map<String, Object> sourceMap = getSourceMap("c", "dynamic_table");
        assertThat(getByPath(sourceMap, "properties.meta.properties.meta.properties.a.type")).isEqualTo("array");
        assertThat(getByPath(sourceMap, "properties.meta.properties.meta.properties.a.inner.type")).isEqualTo("keyword");
    }

    @Test
    public void testInsertMultipleValuesDynamic() throws Exception {
        execute("create table dynamic_table (" +
                "  my_object object " +
                ") with (column_policy='dynamic', number_of_replicas=0)");
        ensureYellow();

        execute("insert into dynamic_table (my_object) values ({a=['a','b']}),({b=['a']})");
        execute("refresh table dynamic_table");

        Map<String, Object> sourceMap = getSourceMap("dynamic_table");
        assertThat(getByPath(sourceMap, "properties.my_object.properties.a.type")).isEqualTo("array");
        assertThat(getByPath(sourceMap, "properties.my_object.properties.a.inner.type")).isEqualTo("keyword");
        assertThat(getByPath(sourceMap, "properties.my_object.properties.b.type")).isEqualTo("array");
        assertThat(getByPath(sourceMap, "properties.my_object.properties.b.inner.type")).isEqualTo("keyword");
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
        Asserts.assertSQLError(() -> execute("insert into books (title, author) values (?,?)",
            new Object[]{
                "Life, the Universe and Everything",
                Map.of("name", Map.of("first_name", "Douglas", "middle_name", "Noel"))
            }))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Cannot add column `middle_name` to strict object `author['name']`");
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
        assertThat(response)
            .hasColumns("id", "name")
            .hasRows("1| Ford");

        execute("update dynamic_table set name='Trillian', boo=true where name='Ford'");
        execute("refresh table dynamic_table");

        execute("select * from dynamic_table");
        assertThat(response)
            .hasColumns("id", "name", "boo")
            .hasRows("1| Trillian| true");
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
        assertThat(response)
            .hasColumns("id", "score")
            .hasRows("1| 42.24");

        execute("insert into dynamic_table (id, score, good) values (2, -0.01, false)");
        execute("refresh table dynamic_table");

        execute("select * from dynamic_table order by id");
        assertThat(response).hasColumns("id", "score", "good");
        assertThat(response).hasRows(
            "1| 42.24| NULL",
            "2| -0.01| false"
        );
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
        assertThat(response)
            .hasColumns("id", "score")
            .hasRows("1| 4656234.345");

        execute("update dynamic_table set name='Trillian', good=true where score > 0.0");
        execute("refresh table dynamic_table");

        execute("select * from dynamic_table");
        assertThat(response)
            .hasColumns("id", "score", "name", "good")
            .hasRows("1| 4656234.345| Trillian| true");
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
        assertThat(response.getIndexTemplates()).hasSize(1);
        IndexTemplateMetadata template = response.getIndexTemplates().get(0);
        CompressedXContent mappingStr = template.mapping();
        assertThat(mappingStr).isNotNull();
        ParsedXContent typeAndMap =
            XContentHelper.convertToMap(mappingStr.compressedReference(), false, XContentType.JSON);
        @SuppressWarnings("unchecked")
        Map<String, Object> mapping = (Map<String, Object>) typeAndMap.map().get(Constants.DEFAULT_MAPPING_TYPE);
        assertThat(ColumnPolicy.fromMappingValue(mapping.get(ColumnPolicy.MAPPING_KEY)))
            .isEqualTo(ColumnPolicy.STRICT);

        execute("insert into numbers (num, odd, prime) values (?, ?, ?)",
            new Object[]{6, true, false});
        execute("refresh table numbers");

        Map<String, Object> sourceMap = getSourceMap(
            new PartitionName(new RelationName("doc", "numbers"), Arrays.asList("true")).asIndexName());
        assertThat(ColumnPolicy.fromMappingValue(sourceMap.get(ColumnPolicy.MAPPING_KEY)))
            .isEqualTo(ColumnPolicy.STRICT);

        Asserts.assertSQLError(() -> execute("insert into numbers (num, odd, prime, perfect) values (?, ?, ?, ?)",
                                   new Object[]{28, true, false, true}))
            .hasPGError(UNDEFINED_COLUMN)
            .hasHTTPError(NOT_FOUND, 4043)
            .hasMessageContaining("Column perfect unknown");
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
        assertThat(response.getIndexTemplates()).hasSize(1);
        IndexTemplateMetadata template = response.getIndexTemplates().get(0);
        CompressedXContent mappingStr = template.mapping();
        assertThat(mappingStr).isNotNull();
        ParsedXContent typeAndMap = XContentHelper.convertToMap(mappingStr.compressedReference(), false, XContentType.JSON);
        @SuppressWarnings("unchecked")
        Map<String, Object> mapping = (Map<String, Object>) typeAndMap.map().get(Constants.DEFAULT_MAPPING_TYPE);
        assertThat(ColumnPolicy.fromMappingValue(mapping.get(ColumnPolicy.MAPPING_KEY)))
            .isEqualTo(ColumnPolicy.STRICT);

        execute("insert into numbers (num, odd, prime) values (?, ?, ?)",
            new Object[]{6, true, false});
        execute("refresh table numbers");

        Map<String, Object> sourceMap = getSourceMap(
            new PartitionName(new RelationName("doc", "numbers"), Arrays.asList("true")).asIndexName());
        assertThat(ColumnPolicy.fromMappingValue(sourceMap.get(ColumnPolicy.MAPPING_KEY))).isEqualTo(ColumnPolicy.STRICT);

        Asserts.assertSQLError(() -> execute("update numbers set num=?, perfect=? where num=6",
                                   new Object[]{28, true}))
            .hasPGError(UNDEFINED_COLUMN)
            .hasHTTPError(NOT_FOUND, 4043)
            .hasMessageContaining("Column perfect unknown");
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
        assertThat(templateResponse.getIndexTemplates()).hasSize(1);
        IndexTemplateMetadata template = templateResponse.getIndexTemplates().get(0);
        CompressedXContent mappingStr = template.mapping();
        assertThat(mappingStr).isNotNull();
        ParsedXContent typeAndMap = XContentHelper.convertToMap(mappingStr.compressedReference(), false, XContentType.JSON);
        @SuppressWarnings("unchecked")
        Map<String, Object> mapping = (Map<String, Object>) typeAndMap.map().get(Constants.DEFAULT_MAPPING_TYPE);
        assertThat(mapping.get("dynamic")).isEqualTo("true");

        execute("insert into numbers (num, odd, prime) values (?, ?, ?)",
            new Object[]{6, true, false});
        execute("refresh table numbers");

        Map<String, Object> sourceMap = getSourceMap(
            new PartitionName(new RelationName("doc", "numbers"), Collections.singletonList("true")).asIndexName());
        assertThat(sourceMap.get("dynamic")).isEqualTo("true");

        execute("insert into numbers (num, odd, prime, perfect) values (?, ?, ?, ?)",
            new Object[]{28, true, false, true});
        ensureYellow();
        execute("refresh table numbers");

        execute("select * from numbers order by num");
        assertThat(response).hasColumns("num", "odd", "prime", "perfect");
        assertThat(response).hasRows(
            "6| true| false| NULL",
            "28| true| false| true"
        );

        execute("update numbers set prime=true, changed='2014-10-23T10:20', author='troll' where num=28");
        assertThat(response).hasRowCount(1);

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
        Asserts.assertSQLError(() -> execute(
                "insert into dynamic_table (id, score, new_col) values (1, 4656234.345, 'hello')"))
            .hasPGError(UNDEFINED_COLUMN)
            .hasHTTPError(NOT_FOUND, 4043)
            .hasMessageContaining("Column new_col unknown");
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
        assertThat(ColumnPolicy.fromMappingValue(getSourceMap("dynamic_table").get(ColumnPolicy.MAPPING_KEY)))
            .isEqualTo(ColumnPolicy.DYNAMIC);
        execute("alter table dynamic_table reset (column_policy)");
        waitNoPendingTasksOnAll();
        assertThat(ColumnPolicy.fromMappingValue(getSourceMap("dynamic_table").get(ColumnPolicy.MAPPING_KEY)))
            .isEqualTo(ColumnPolicy.STRICT);
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
        assertThat(ColumnPolicy.fromMappingValue(sourceMap.get(ColumnPolicy.MAPPING_KEY))).isEqualTo(ColumnPolicy.DYNAMIC);
        execute("alter table dynamic_table reset (column_policy)");
        waitNoPendingTasksOnAll();
        sourceMap = getSourceMap(indexName);
        assertThat(ColumnPolicy.fromMappingValue(sourceMap.get(ColumnPolicy.MAPPING_KEY))).isEqualTo(ColumnPolicy.STRICT);
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
        assertThat(response.getIndexTemplates()).hasSize(1);
        IndexTemplateMetadata template = response.getIndexTemplates().get(0);
        CompressedXContent mappingStr = template.mapping();
        assertThat(mappingStr).isNotNull();
        ParsedXContent typeAndMap =
            XContentHelper.convertToMap(mappingStr.compressedReference(), false, XContentType.JSON);
        @SuppressWarnings("unchecked")
        Map<String, Object> mapping = (Map<String, Object>) typeAndMap.map().get(Constants.DEFAULT_MAPPING_TYPE);
        assertThat(ColumnPolicy.fromMappingValue(mapping.get("dynamic"))).isEqualTo(ColumnPolicy.DYNAMIC);

        execute("insert into dynamic_table (id, score, new_col) values (?, ?, ?)",
            new Object[]{6, 3, "hello"});
        execute("refresh table dynamic_table");
        ensureYellow();

        Map<String, Object> sourceMap = getSourceMap(
            new PartitionName(new RelationName("doc", "dynamic_table"), Arrays.asList("10.0")).asIndexName());
        assertThat(ColumnPolicy.fromMappingValue(sourceMap.get(ColumnPolicy.MAPPING_KEY))).isEqualTo(ColumnPolicy.DYNAMIC);

        sourceMap = getSourceMap(new PartitionName(
            new RelationName("doc", "dynamic_table"), Arrays.asList("5.0")).asIndexName());
        assertThat(ColumnPolicy.fromMappingValue(sourceMap.get(ColumnPolicy.MAPPING_KEY))).isEqualTo(ColumnPolicy.DYNAMIC);

        sourceMap = getSourceMap(new PartitionName(
            new RelationName("doc", "dynamic_table"), Arrays.asList("3.0")).asIndexName());
        assertThat(ColumnPolicy.fromMappingValue(sourceMap.get(ColumnPolicy.MAPPING_KEY))).isEqualTo(ColumnPolicy.DYNAMIC);
    }

}
