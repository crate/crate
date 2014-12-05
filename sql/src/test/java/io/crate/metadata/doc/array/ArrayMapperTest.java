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

package io.crate.metadata.doc.array;

import com.google.common.base.Joiner;
import io.crate.test.integration.CrateSingleNodeTest;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.core.ArrayMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static org.hamcrest.Matchers.*;

public class ArrayMapperTest extends CrateSingleNodeTest {

    public static final String INDEX = "my_index";
    public static final String TYPE = "type";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * create index with type and mapping and validate DocumentMapper serialization
     */
    private DocumentMapper mapper(String indexName, String type, String mapping) throws IOException {
        // we serialize and deserialize the mapping to make sure serialization works just fine
        client().admin().indices().prepareCreate(indexName)
                .addMapping(type, mapping)
                .setSettings(ImmutableSettings.builder().put("number_of_replicas", 0).build()).execute().actionGet();
        client().admin().cluster().prepareHealth(indexName)
                .setWaitForGreenStatus()
                .setWaitForRelocatingShards(0)
                .setWaitForEvents(Priority.LANGUID).execute().actionGet();
        IndicesService instanceFromNode = getInstanceFromNode(IndicesService.class);
        IndexService indexService = instanceFromNode.indexServiceSafe(indexName);
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();

        DocumentMapper defaultMapper = parser.parse(mapping);
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        defaultMapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String rebuildMapping = builder.string();
        return parser.parse(rebuildMapping);
    }

    @Test
    public void testSimpleArrayMapping() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                .startObject().startObject(TYPE).startObject("properties")
                    .startObject("array_field")
                        .field("type", ArrayMapper.CONTENT_TYPE)
                        .startObject(ArrayMapper.INNER)
                            .field("type", "string")
                            .field("index", "not_analyzed")
                        .endObject()
                    .endObject()
                .endObject().endObject().endObject()
                .string();
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);

        // child field mapper
        assertThat(mapper.mappers().name("array_field").mappers().get(0), is(instanceOf(StringFieldMapper.class)));
        ParsedDocument doc = mapper.parse(XContentFactory.jsonBuilder()
                .startObject()
                .field("_id", "abc")
                .array("array_field", "a", "b", "c")
                .endObject()
                .bytes());
        assertThat(doc.mappingsModified(), is(false));
        assertThat(doc.docs().size(), is(1));
        assertThat(doc.docs().get(0).getValues("array_field"), arrayContainingInAnyOrder("a", "b", "c"));
        assertThat(
                mapper.mappingSource().string(),
                is("{\"type\":{" +
                        "\"properties\":{" +
                            "\"array_field\":{" +
                                "\"type\":\"array\"," +
                                "\"inner\":{" +
                                    "\"type\":\"string\"," +
                                    "\"index\":\"not_analyzed\"}" +
                                "}" +
                            "}" +
                        "}" +
                        "}"));
    }

    @Test
    public void testInvalidArraySimpleField() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type").startObject("properties")
                .startObject("array_field")
                .field("type", ArrayMapper.CONTENT_TYPE)
                .startObject(ArrayMapper.INNER)
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .endObject()
                .endObject().endObject().endObject()
                .string();
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);

        try {
            mapper.parse(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("_id", "abc")
                    .field("array_field", "a")
                    .endObject()
                    .bytes());
            fail("array_field parsed simple field");
        } catch (MapperParsingException e) {
            assertThat(e.getCause(), instanceOf(ElasticsearchParseException.class));
            assertThat(e.getCause().getMessage(), is("invalid array"));
        }
    }

    @Test
    public void testInvalidArrayNonConvertableType() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type").startObject("properties")
                .startObject("array_field")
                .field("type", ArrayMapper.CONTENT_TYPE)
                .startObject(ArrayMapper.INNER)
                    .field("type", "double")
                    .field("index", "not_analyzed")
                .endObject()
                .endObject()
                .endObject().endObject().endObject()
                .string();
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);

        expectedException.expect(MapperParsingException.class);
        expectedException.expectMessage("failed to parse [array_field]");

        mapper.parse(XContentFactory.jsonBuilder()
                .startObject()
                .field("_id", "abc")
                .array("array_field", true, false, true)
                .endObject()
                .bytes());
    }

    @Test
    public void testObjectArrayMapping() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type").startObject("properties")
                .startObject("array_field")
                    .field("type", ArrayMapper.CONTENT_TYPE)
                    .startObject(ArrayMapper.INNER)
                        .field("type", "object")
                        .field("dynamic", true)
                        .startObject("properties")
                            .startObject("s")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject()
                .string();
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);
        // child object mapper
        assertThat(mapper.objectMappers().get("array_field"), is(instanceOf(ObjectMapper.class)));
        ParsedDocument doc = mapper.parse(XContentFactory.jsonBuilder()
                .startObject()
                .field("_id", "abc")
                .startArray("array_field")
                .startObject()
                .field("s", "a")
                .endObject()
                .startObject()
                .field("s", "b")
                .endObject()
                .startObject()
                .field("s", "c")
                .endObject()
                .endArray()
                .endObject()
                .bytes());
        assertThat(doc.mappingsModified(), is(false));
        assertThat(doc.docs().size(), is(1));
        assertThat(doc.docs().get(0).getValues("array_field.s"), arrayContainingInAnyOrder("a", "b", "c"));
        assertThat(mapper.mappers().smartNameFieldMapper("array_field.s"), instanceOf(StringFieldMapper.class));
        assertThat(
                mapper.mappingSource().string(),
                is("{\"type\":{\"properties\":{" +
                        "\"array_field\":{" +
                          "\"type\":\"array\"," +
                          "\"inner\":{" +
                            "\"dynamic\":\"true\"," +
                            "\"properties\":{" +
                               "\"s\":{" +
                                 "\"type\":\"string\"," +
                                 "\"index\":\"not_analyzed\"" +
                               "}" +
                            "}" +
                          "}" +
                        "}" +
                    "}}}"));
    }

    @Test
    public void testObjectArrayMappingNewColumn() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type").startObject("properties")
                .startObject("array_field")
                .field("type", ArrayMapper.CONTENT_TYPE)
                .startObject(ArrayMapper.INNER)
                    .field("type", "object")
                    .field("dynamic", true)
                    .startObject("properties")
                        .startObject("s")
                            .field("type", "string")
                            .field("index", "not_analyzed")
                        .endObject()
                    .endObject()
                .endObject()
                .endObject()
                .endObject().endObject().endObject()
                .string();
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);
        // child object mapper
        assertThat(mapper.objectMappers().get("array_field"), is(instanceOf(ObjectMapper.class)));
        ParsedDocument doc = mapper.parse(XContentFactory.jsonBuilder()
                .startObject()
                    .field("_id", "abc")
                    .startArray("array_field")
                        .startObject()
                            .field("s", "a")
                            .field("new", true)
                        .endObject()
                    .endArray()
                .endObject()
                .bytes());
        assertThat(doc.mappingsModified(), is(true));
        assertThat(doc.docs().size(), is(1));
        assertThat(doc.docs().get(0).getValues("array_field.new"), arrayContainingInAnyOrder("T"));
        mapper.refreshSource();
        assertThat(
                mapper.mappingSource().string(),
                is("{\"type\":{\"properties\":{" +
                        "\"array_field\":{" +
                            "\"type\":\"array\"," +
                            "\"inner\":{" +
                                "\"dynamic\":\"true\"," +
                                "\"properties\":{" +
                                    "\"new\":{\"type\":\"boolean\"}," +
                                    "\"s\":{" +
                                        "\"type\":\"string\"," +
                                        "\"index\":\"not_analyzed\"" +
                                    "}" +
                                "}" +
                            "}" +
                        "}" +
                        "}}}"));
    }

    @Test
    public void testInsertGetArray() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type").startObject("properties")
                .startObject("array_field")
                .field("type", ArrayMapper.CONTENT_TYPE)
                .startObject(ArrayMapper.INNER)
                .field("type", "double")
                .field("index", "not_analyzed")
                .endObject()
                .endObject()
                .endObject().endObject().endObject()
                .string();
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);
        IndexResponse response = client().prepareIndex(INDEX, "type").setId("123").setSource("{array_field:[0.0, 99.9, -100.5678]}").execute().actionGet();
        assertThat(response.getVersion(), is(1L));

        client().admin().indices().prepareRefresh(INDEX).execute().actionGet();

        // realtime
        GetResponse rtGetResponse = client().prepareGet(INDEX, "type", "123")
                .setFetchSource(true).setFields("array_field").setRealtime(true).execute().actionGet();
        assertThat(rtGetResponse.getId(), is("123"));
        assertThat(Joiner.on(',').withKeyValueSeparator(":").join(rtGetResponse.getSource()), is("array_field:[0.0, 99.9, -100.5678]"));
        assertThat(rtGetResponse.getField("array_field").getValues(), Matchers.<Object>hasItems(0.0D, 99.9D, -100.5678D));

        // non-realtime
        GetResponse getResponse = client().prepareGet(INDEX, "type", "123")
                .setFetchSource(true).setFields("array_field").setRealtime(false).execute().actionGet();
        assertThat(getResponse.getId(), is("123"));
        assertThat(Joiner.on(',').withKeyValueSeparator(":").join(getResponse.getSource()), is("array_field:[0.0, 99.9, -100.5678]"));
        assertThat(getResponse.getField("array_field").getValues(), Matchers.<Object>hasItems(0.0D, 99.9D, -100.5678D));
    }

    @Test
    public void testInsertSearchArray() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type").startObject("properties")
                .startObject("array_field")
                .field("type", ArrayMapper.CONTENT_TYPE)
                .startObject(ArrayMapper.INNER)
                .field("type", "double")
                .field("index", "not_analyzed")
                .endObject()
                .endObject()
                .endObject().endObject().endObject()
                .string();
        mapper(INDEX, TYPE, mapping);
        IndexResponse response = client().prepareIndex(INDEX, "type").setId("123").setSource("{array_field:[0.0, 99.9, -100.5678]}").execute().actionGet();
        assertThat(response.getVersion(), is(1L));

        client().admin().indices().prepareRefresh(INDEX).execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch(INDEX).setTypes("type")
                .setFetchSource(true).addField("array_field")
                .setQuery("{\"term\": {\"array_field\": 0.0}}").execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), is(1L));
        assertThat(Joiner.on(',').withKeyValueSeparator(":").join(
                searchResponse.getHits().getAt(0).getSource()),
                is("array_field:[0.0, 99.9, -100.5678]"));
        assertThat(searchResponse.getHits().getAt(0).field("array_field").getValues(), Matchers.<Object>hasItems(0.0D, 99.9D, -100.5678D));
    }

    @Test
    public void testEmptyArray() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type").startObject("properties")
                .startObject("array_field")
                    .field("type", ArrayMapper.CONTENT_TYPE)
                    .startObject(ArrayMapper.INNER)
                        .field("type", "double")
                        .field("index", "not_analyzed")
                    .endObject()
                .endObject()
                .endObject().endObject().endObject()
                .string();
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);

        // parse source with empty array
        ParsedDocument doc = mapper.parse(XContentFactory.jsonBuilder()
                .startObject()
                .field("_id", "abc")
                .array("array_field")
                .endObject()
                .bytes());
        assertThat(doc.mappingsModified(), is(false));
        assertThat(doc.docs().size(), is(1));
        assertThat(doc.docs().get(0).get("array_field"), is(nullValue())); // no lucene field generated

        // insert
        IndexResponse response = client().prepareIndex(INDEX, "type", "123").setSource("{array_field:[]}").execute().actionGet();
        assertThat(response.getVersion(), is(1L));

        client().admin().indices().prepareRefresh(INDEX).execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch(INDEX).setTypes("type")
                .setFetchSource(true).addField("array_field")
                .setQuery("{\"term\": {\"_id\": \"123\"}}").execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), is(1L));
        assertThat(Joiner.on(',').withKeyValueSeparator(":").join(
                        searchResponse.getHits().getAt(0).getSource()),
                is("array_field:[]"));
        assertThat(searchResponse.getHits().getAt(0).fields().containsKey("array_field"), is(false));
    }

    @Test
    public void testNestedArrayMapping() throws Exception {
        expectedException.expect(MapperParsingException.class);
        expectedException.expectMessage("mapping [type]");
        expectedException.expectCause(
                TestingHelpers.cause(MapperParsingException.class, "nested arrays are not supported"));


        String mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type").startObject("properties")
                    .startObject("array_field")
                    .field("type", ArrayMapper.CONTENT_TYPE)
                    .startObject(ArrayMapper.INNER)
                        .field("type", "array")
                        .startObject("inner")
                            .field("type", "double")
                            .field("index", "not_analyzed")
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject()
                .string();
        mapper(INDEX, TYPE, mapping);
    }

    @Test
    public void testParseDynamicNestedArray() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type").startObject("properties")
                .startObject("array_field")
                    .field("type", ArrayMapper.CONTENT_TYPE)
                    .startObject(ArrayMapper.INNER)
                        .field("type", "double")
                        .field("index", "not_analyzed")
                    .endObject()
                .endObject()
                .endObject().endObject().endObject()
                .string();
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);

        expectedException.expect(MapperParsingException.class);
        expectedException.expectMessage("failed to parse");
        expectedException.expectCause(TestingHelpers.cause(ElasticsearchParseException.class, "nested arrays are not supported"));
        // parse source with empty array
        mapper.parse(XContentFactory.jsonBuilder()
                .startObject()
                .field("_id", "abc")
                .startArray("new_array_field")
                    .startArray().value("a").value("b").endArray()
                .endArray()
                .endObject()
                .bytes());

    }

    @Test
    public void testParseNull() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type").startObject("properties")
                .startObject("array_field")
                .field("type", ArrayMapper.CONTENT_TYPE)
                .startObject(ArrayMapper.INNER)
                .field("type", "double")
                .field("index", "not_analyzed")
                .endObject()
                .endObject()
                .endObject().endObject().endObject()
                .string();
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);
        ParsedDocument parsedDoc = mapper.parse(XContentFactory.jsonBuilder()
                .startObject()
                    .field("_id", "abc")
                    .nullField("array_field")
                .endObject()
                .bytes());
        assertThat(parsedDoc.docs().size(), is(1));
        assertThat(parsedDoc.docs().get(0).getField("array_field"), is(nullValue()));
    }

    @Test
    public void testParseDynamicNullArray() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type").startObject("properties")
                .endObject().endObject().endObject()
                .string();
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);

        expectedException.expect(MapperParsingException.class);
        expectedException.expectCause(TestingHelpers.cause(ElasticsearchIllegalStateException.class,
                "Can't handle serializing a dynamic type with content token [VALUE_NULL] and field name [new_array_field]"));
        // parse source with null array
        mapper.parse(XContentFactory.jsonBuilder()
                .startObject()
                .field("_id", "abc")
                .startArray("new_array_field").nullValue()
                .endArray()
                .endObject()
                .bytes());
    }
}
