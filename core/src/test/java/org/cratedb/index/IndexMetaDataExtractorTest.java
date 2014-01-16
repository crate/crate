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
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.cratedb.index;

import org.cratedb.Constants;
import org.cratedb.DataType;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class IndexMetaDataExtractorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private IndexMetaData getIndexMetaData(String indexName, XContentBuilder builder) throws IOException {
        return getIndexMetaData(indexName, builder, ImmutableSettings.Builder.EMPTY_SETTINGS);
    }
    private IndexMetaData getIndexMetaData(String indexName, XContentBuilder builder, Settings settings)
        throws IOException
    {
        byte[] data = builder.bytes().toBytes();
        Map<String, Object> mappingSource = XContentHelper.convertToMap(data, true).v2();
        mappingSource = sortProperties(mappingSource);

        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put(settings);

        return IndexMetaData.builder(indexName)
            .settings(settingsBuilder)
            .putMapping(new MappingMetaData(Constants.DEFAULT_MAPPING_TYPE, mappingSource))
            .build();
    }

    @Test
    public void testExtractColumnDefinitions() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                        .startObject("_meta")
                            .field("primary_keys", "id")
                        .endObject()
                        .startObject("properties")
                            .startObject("id")
                                .field("type", "integer")
                                .field("index", "not_analyzed")
                            .endObject()
                            .startObject("title")
                                .field("type", "string")
                                .field("index", "no")
                            .endObject()
                            .startObject("datum")
                                .field("type", "date")
                            .endObject()
                            .startObject("content")
                                .field("type", "string")
                                .field("index", "analyzed")
                                .field("analyzer", "standard")
                            .endObject()
                            .startObject("person")
                                .startObject("properties")
                                    .startObject("first_name")
                                        .field("type", "string")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                    .startObject("birthday")
                                        .field("type", "date")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                .endObject()
                            .endObject()
                            .startObject("nested")
                                .field("type", "nested")
                                .startObject("properties")
                                    .startObject("inner_nested")
                                        .field("type", "date")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                .endObject();


        IndexMetaData metaData = getIndexMetaData("test1", builder);
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        List<ColumnDefinition> columnDefinitions = extractor.getColumnDefinitions();

        assertEquals(9, columnDefinitions.size());

        assertThat(columnDefinitions.get(0).columnName, is("content"));
        assertThat(columnDefinitions.get(0).dataType, is(DataType.STRING));
        assertThat(columnDefinitions.get(0).ordinalPosition, is(1));
        assertThat(columnDefinitions.get(0).tableName, is("test1"));
        assertFalse(columnDefinitions.get(0).dynamic);
        assertTrue(columnDefinitions.get(0).strict);

        assertThat(columnDefinitions.get(1).columnName, is("datum"));
        assertThat(columnDefinitions.get(1).dataType, is(DataType.TIMESTAMP));
        assertThat(columnDefinitions.get(1).ordinalPosition, is(2));
        assertThat(columnDefinitions.get(1).tableName, is("test1"));
        assertFalse(columnDefinitions.get(1).dynamic);
        assertTrue(columnDefinitions.get(1).strict);

        assertThat(columnDefinitions.get(2).columnName, is("id"));
        assertThat(columnDefinitions.get(2).dataType, is(DataType.INTEGER));
        assertThat(columnDefinitions.get(2).ordinalPosition, is(3));
        assertThat(columnDefinitions.get(2).tableName, is("test1"));
        assertFalse(columnDefinitions.get(2).dynamic);
        assertTrue(columnDefinitions.get(2).strict);

        assertThat(columnDefinitions.get(3).columnName, is("nested"));
        assertThat(columnDefinitions.get(3).dataType, is(DataType.OBJECT));
        assertThat(columnDefinitions.get(3).ordinalPosition, is(4));
        assertThat(columnDefinitions.get(3).tableName, is("test1"));
        assertTrue(columnDefinitions.get(3).dynamic);
        assertFalse(columnDefinitions.get(3).strict);

        assertThat(columnDefinitions.get(4).columnName, is("nested.inner_nested"));
        assertThat(columnDefinitions.get(4).dataType, is(DataType.TIMESTAMP));
        assertThat(columnDefinitions.get(4).ordinalPosition, is(5));
        assertThat(columnDefinitions.get(4).tableName, is("test1"));
        assertFalse(columnDefinitions.get(4).dynamic);
        assertTrue(columnDefinitions.get(4).strict);

        assertThat(columnDefinitions.get(5).columnName, is("person"));
        assertThat(columnDefinitions.get(5).dataType, is(DataType.OBJECT));
        assertThat(columnDefinitions.get(5).ordinalPosition, is(6));
        assertThat(columnDefinitions.get(5).tableName, is("test1"));
        assertTrue(columnDefinitions.get(5).dynamic);
        assertFalse(columnDefinitions.get(5).strict);

        assertThat(columnDefinitions.get(6).columnName, is("person.birthday"));
        assertThat(columnDefinitions.get(6).dataType, is(DataType.TIMESTAMP));
        assertThat(columnDefinitions.get(6).ordinalPosition, is(7));
        assertThat(columnDefinitions.get(6).tableName, is("test1"));
        assertFalse(columnDefinitions.get(6).dynamic);
        assertTrue(columnDefinitions.get(6).strict);

        assertThat(columnDefinitions.get(7).columnName, is("person.first_name"));
        assertThat(columnDefinitions.get(7).dataType, is(DataType.STRING));
        assertThat(columnDefinitions.get(7).ordinalPosition, is(8));
        assertThat(columnDefinitions.get(7).tableName, is("test1"));
        assertFalse(columnDefinitions.get(7).dynamic);
        assertTrue(columnDefinitions.get(7).strict);

        assertThat(columnDefinitions.get(8).columnName, is("title"));
        assertThat(columnDefinitions.get(8).dataType, is(DataType.STRING));
        assertThat(columnDefinitions.get(8).ordinalPosition, is(9));
        assertThat(columnDefinitions.get(8).tableName, is("test1"));
        assertFalse(columnDefinitions.get(8).dynamic);
        assertTrue(columnDefinitions.get(8).strict);
    }

    private Map<String, Object> sortProperties(Map<String, Object> mappingSource) {
        return sortProperties(mappingSource, false);
    }

    /**
     * in the DocumentMapper that ES uses at some place the properties of the mapping are sorted.
     * this logic doesn't seem to be triggered if the IndexMetaData is created using the
     * IndexMetaData.Builder.
     *
     * in order to have the same behaviour as if a Node was started and a index with mapping was created
     * using the ES tools pre-sort the mapping here.
     */
    @SuppressWarnings("unchecked")
    private Map<String,Object> sortProperties(Map<String, Object> mappingSource, boolean doSort) {
        Map<String, Object> map;
        if (doSort) {
            map = new TreeMap<>();
        } else {
            map = new HashMap<>();
        }

        boolean sortNext;
        Object value;
        for (Map.Entry<String, Object> entry : mappingSource.entrySet()) {
            value = entry.getValue();
            sortNext = entry.getKey().equals("properties");

            if (value instanceof Map) {
                map.put(entry.getKey(), sortProperties((Map) entry.getValue(), sortNext));
            } else {
                map.put(entry.getKey(), entry.getValue());
            }
        }

        return map;
    }

    @Test
    public void testExtractColumnDefinitionsFromEmptyIndex() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .endObject()
            .endObject();
        IndexMetaData metaData = getIndexMetaData("test2", builder);

        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        List<ColumnDefinition> columnDefinitions = extractor.getColumnDefinitions();
        assertEquals(0, columnDefinitions.size());
    }

    @Test
    public void testExtractPrimaryKey() throws Exception {

        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(Constants.DEFAULT_MAPPING_TYPE)
                        .startObject("_meta")
                            .field("primary_keys", "id")
                        .endObject()
                        .startObject("properties")
                            .startObject("id")
                                .field("type", "integer")
                                .field("index", "not_analyzed")
                            .endObject()
                            .startObject("title")
                                .field("type", "string")
                                .field("index", "no")
                            .endObject()
                            .startObject("datum")
                                .field("type", "date")
                            .endObject()
                            .startObject("content")
                                .field("type", "string")
                                .field("index", "analyzed")
                                .field("analyzer", "standard")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        IndexMetaData metaData = getIndexMetaData("test3", builder);
        IndexMetaDataExtractor extractor1 = new IndexMetaDataExtractor(metaData);
        assertThat(extractor1.getPrimaryKeys().size(), is(1));
        assertThat(extractor1.getPrimaryKeys(), contains("id"));

        builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(Constants.DEFAULT_MAPPING_TYPE)
                        .startObject("properties")
                            .startObject("content")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        IndexMetaDataExtractor extractor2 = new IndexMetaDataExtractor(getIndexMetaData("test4", builder));
        assertThat(extractor2.getPrimaryKeys().size(), is(0));

        builder = XContentFactory.jsonBuilder()
            .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .endObject()
            .endObject();
        IndexMetaDataExtractor extractor3 = new IndexMetaDataExtractor(getIndexMetaData("test5", builder));
        assertThat(extractor3.getPrimaryKeys().size(), is(0));
    }

    @Test
    public void testGetIndices() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(Constants.DEFAULT_MAPPING_TYPE)
                        .startObject("_meta")
                            .field("primary_keys", "id")
                        .endObject()
                        .startObject("properties")
                        .startObject("id")
                            .field("type", "integer")
                            .field("index", "not_analyzed")
                        .endObject()
                        .startObject("title")
                            .field("type", "multi_field")
                            .field("path", "just_name")
                            .startObject("fields")
                                .startObject("title")
                                    .field("type", "string")
                                    .field("index", "not_analyzed")
                                .endObject()
                                .startObject("ft")
                                    .field("type", "string")
                                    .field("index", "analyzed")
                                    .field("analyzer", "english")
                                .endObject()
                            .endObject()
                        .endObject()
                        .startObject("datum")
                            .field("type", "date")
                        .endObject()
                        .startObject("content")
                            .field("type", "multi_field")
                            .field("path", "just_name")
                            .startObject("fields")
                                .startObject("content")
                                    .field("type", "string")
                                    .field("index", "no")
                                .endObject()
                                .startObject("ft")
                                    .field("type", "string")
                                    .field("index", "analyzed")
                                    .field("analyzer", "english")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        IndexMetaData metaData = getIndexMetaData("test6", builder);
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        List<IndexMetaDataExtractor.Index> indices = extractor.getIndices();

        assertEquals(5, indices.size());

        assertThat(indices.get(0).indexName, is("ft"));
        assertThat(indices.get(0).tableName, is("test6"));
        assertThat(indices.get(0).method, is("fulltext"));
        assertThat(indices.get(0).getColumnsString(), is("content, title"));
        assertThat(indices.get(0).getPropertiesString(), is("analyzer=english"));
        assertThat(indices.get(0).getUid(), is("test6.ft"));

        assertThat(indices.get(1).indexName, is("datum"));
        assertThat(indices.get(1).tableName, is("test6"));
        assertThat(indices.get(1).method, is("plain"));
        assertThat(indices.get(1).getColumnsString(), is("datum"));
        assertThat(indices.get(1).getUid(), is("test6.datum"));

        assertThat(indices.get(2).indexName, is("id"));
        assertThat(indices.get(2).tableName, is("test6"));
        assertThat(indices.get(2).getColumnsString(), is("id"));
        assertThat(indices.get(2).method, is("plain"));

        assertThat(indices.get(3).indexName, is("title"));
        assertThat(indices.get(3).tableName, is("test6"));
        assertThat(indices.get(3).getColumnsString(), is("title"));
        assertThat(indices.get(3).method, is("plain"));

        // compound indices will be listed once for every column they index,
        // will be merged by indicesTable
        assertThat(indices.get(4).indexName, is("ft"));
    }

    @Test
    public void extractIndicesFromEmptyIndex() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .endObject()
            .endObject();
        IndexMetaData metaData = getIndexMetaData("test7", builder);
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        List<IndexMetaDataExtractor.Index> indices = extractor.getIndices();
        assertEquals(0, indices.size());

    }

    @Test
    public void extractRoutingColumn() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(Constants.DEFAULT_MAPPING_TYPE)
                        .startObject("_meta")
                            .field("primary_keys", "id")
                        .endObject()
                        .startObject("properties")
                            .startObject("id")
                                .field("type", "integer")
                                .field("index", "not_analyzed")
                            .endObject()
                            .startObject("title")
                                .field("type", "multi_field")
                                .field("path", "just_name")
                                .startObject("fields")
                                    .startObject("title")
                                        .field("type", "string")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                    .startObject("ft")
                                        .field("type", "string")
                                        .field("index", "analyzed")
                                        .field("analyzer", "english")
                                    .endObject()
                                .endObject()
                            .endObject()
                            .startObject("datum")
                                .field("type", "date")
                            .endObject()
                            .startObject("content")
                                .field("type", "multi_field")
                                .field("path", "just_name")
                                .startObject("fields")
                                    .startObject("content")
                                        .field("type", "string")
                                        .field("index", "no")
                                    .endObject()
                                    .startObject("ft")
                                        .field("type", "string")
                                        .field("index", "analyzed")
                                        .field("analyzer", "english")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        IndexMetaData metaData = getIndexMetaData("test8", builder);
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);

        assertThat(extractor.getRoutingColumn(), is("id"));

        builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(Constants.DEFAULT_MAPPING_TYPE)
                        .startObject("properties")
                            .startObject("content")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        metaData = getIndexMetaData("test9", builder);
        extractor = new IndexMetaDataExtractor(metaData);

        assertThat(extractor.getRoutingColumn(), is("_id"));

        builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(Constants.DEFAULT_MAPPING_TYPE)
                        .startObject("_meta")
                            .field("primary_keys", "id")
                        .endObject()
                        .startObject("_routing")
                            .field("path", "id")
                        .endObject()
                        .startObject("properties")
                            .startObject("id")
                                .field("type", "integer")
                                .field("index", "not_analyzed")
                            .endObject()
                            .startObject("content")
                                .field("type", "string")
                                .field("index", "no")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        metaData = getIndexMetaData("test10", builder);
        extractor = new IndexMetaDataExtractor(metaData);

        assertThat(extractor.getRoutingColumn(), is("id"));
    }

    @Test
    public void extractRoutingColumnFromEmptyIndex() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .endObject()
            .endObject();
        IndexMetaData metaData = getIndexMetaData("test11", builder);
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        assertThat(extractor.getRoutingColumn(), is("_id"));
    }

    @Test
    public void testExtractObjectColumn() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(Constants.DEFAULT_MAPPING_TYPE)
                        .startObject("properties")
                            .startObject("craty_field")
                                .startObject("properties")
                                    .startObject("size")
                                        .field("type", "byte")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                    .startObject("created")
                                        .field("type", "date")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                .endObject()
                            .endObject()
                            .startObject("strict_field")
                                .field("type", "object")
                                .field("dynamic", "strict")
                                .startObject("properties")
                                    .startObject("path")
                                        .field("type", "string")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                    .startObject("created")
                                        .field("type", "date")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                .endObject()
                            .endObject()
                            .startObject("no_dynamic_field")
                                .field("type", "object")
                                .field("dynamic", false)
                                .startObject("properties")
                                    .startObject("path")
                                        .field("type", "string")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                    .startObject("dynamic_again")
                                        .startObject("properties")
                                            .startObject("field")
                                                .field("type", "date")
                                            .endObject()
                                        .endObject()
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        IndexMetaData metaData = getIndexMetaData("test12", builder);
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        List<ColumnDefinition> columns = extractor.getColumnDefinitions();

        assertEquals(10, columns.size());

        assertThat(columns.get(0).columnName, is("craty_field"));
        assertThat(columns.get(0).dynamic, is(true));
        assertThat(columns.get(0).dataType, is(DataType.OBJECT));
        assertThat(columns.get(0).ordinalPosition, is(1));
        assertThat(columns.get(0).tableName, is("test12"));
        assertTrue(columns.get(0) instanceof ObjectColumnDefinition);
        ObjectColumnDefinition objectColumn = (ObjectColumnDefinition)columns.get(0);
        assertEquals(2, objectColumn.nestedColumns.size());

        assertEquals(columns.get(1), objectColumn.nestedColumns.get(0));
        assertThat(columns.get(1).columnName, is("craty_field.created"));
        assertThat(columns.get(1).dataType, is(DataType.TIMESTAMP));
        assertThat(columns.get(1).ordinalPosition, is(2));
        assertFalse(columns.get(1).dynamic);
        assertThat(columns.get(1).tableName, is("test12"));

        assertEquals(columns.get(2), objectColumn.nestedColumns.get(1));
        assertThat(columns.get(2).columnName, is("craty_field.size"));
        assertThat(columns.get(2).dataType, is(DataType.BYTE));
        assertThat(columns.get(2).ordinalPosition, is(3));
        assertFalse(columns.get(2).dynamic);
        assertThat(columns.get(2).tableName, is("test12"));

        assertThat(columns.get(3).columnName, is("no_dynamic_field"));
        assertThat(columns.get(3).dynamic, is(false));
        assertThat(columns.get(3).dataType, is(DataType.OBJECT));
        assertThat(columns.get(3).ordinalPosition, is(4));
        assertThat(columns.get(3).tableName, is("test12"));
        assertTrue(columns.get(3) instanceof ObjectColumnDefinition);
        objectColumn = (ObjectColumnDefinition)columns.get(3);
        assertEquals(3, objectColumn.nestedColumns.size());

        assertEquals(columns.get(4), objectColumn.nestedColumns.get(0));
        assertThat(columns.get(4).columnName, is("no_dynamic_field.dynamic_again"));
        assertThat(columns.get(4).dataType, is(DataType.OBJECT));
        assertThat(columns.get(4).ordinalPosition, is(5));
        assertTrue(columns.get(4).dynamic);
        assertThat(columns.get(4).tableName, is("test12"));

        assertEquals(columns.get(5), objectColumn.nestedColumns.get(1));
        assertThat(columns.get(5).columnName, is("no_dynamic_field.dynamic_again.field"));
        assertThat(columns.get(5).dataType, is(DataType.TIMESTAMP));
        assertThat(columns.get(5).ordinalPosition, is(6));
        assertFalse(columns.get(5).dynamic);
        assertThat(columns.get(5).tableName, is("test12"));

        assertEquals(columns.get(6), objectColumn.nestedColumns.get(2));
        assertThat(columns.get(6).columnName, is("no_dynamic_field.path"));
        assertThat(columns.get(6).dataType, is(DataType.STRING));
        assertThat(columns.get(6).ordinalPosition, is(7));
        assertFalse(columns.get(6).dynamic);
        assertThat(columns.get(6).tableName, is("test12"));

    }

    @Test
    public void testIsDynamic() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .startObject("properties")
                    .startObject("field")
                        .field("type", "string")
                    .endObject()
                .endObject()
                .endObject()
                .endObject();
        IndexMetaData metaData = getIndexMetaData("test13", builder,
            ImmutableSettings.builder().put("index.mapper.dynamic", false).build());
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        assertFalse(extractor.isDynamic());

        builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .field("dynamic", true)
                .startObject("properties")
                .startObject("field")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        metaData = getIndexMetaData("test13", builder,
            ImmutableSettings.builder().put("index.mapper.dynamic", false).build());
        extractor = new IndexMetaDataExtractor(metaData);
        assertFalse(extractor.isDynamic());

        builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(Constants.DEFAULT_MAPPING_TYPE)
                        .field("dynamic", false)
                        .startObject("properties")
                            .startObject("field")
                                .field("type", "string")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        metaData = getIndexMetaData("test15", builder);
        extractor = new IndexMetaDataExtractor(metaData);
        assertFalse(extractor.isDynamic());

        builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("field")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        metaData = getIndexMetaData("test16", builder);
        extractor = new IndexMetaDataExtractor(metaData);
        assertFalse(extractor.isDynamic());

        builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .startObject("properties")
                .startObject("field")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        metaData = getIndexMetaData("test17", builder);
        extractor = new IndexMetaDataExtractor(metaData);
        assertTrue(extractor.isDynamic());

    }
}
