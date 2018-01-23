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
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.test.CauseMatcher;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.ArrayMapper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ObjectArrayMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ArrayMapperTest extends SQLTransportIntegrationTest {

    public static final String INDEX = "my_index";
    public static final String TYPE = "type";

    /**
     * create index with type and mapping and validate DocumentMapper serialization
     */
    private DocumentMapper mapper(String indexName, String type, String mapping) throws IOException {
        // we serialize and deserialize the mapping to make sure serialization works just fine
        client().admin().indices().prepareCreate(indexName)
            .setWaitForActiveShards(1)
            .addMapping(type, mapping, XContentType.JSON)
            .setSettings(Settings.builder()
                .put("number_of_replicas", 0)
                .put("number_of_shards", 1).build()).execute().actionGet();
        String[] nodeNames = internalCluster().getNodeNames();
        assert nodeNames.length == 1 : "must have only 1 node, got: " + Arrays.toString(nodeNames);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        Index index = clusterService.state().getMetaData().index(indexName).getIndex();
        IndicesService instanceFromNode = internalCluster().getInstance(IndicesService.class);
        IndexService indexService = instanceFromNode.indexServiceSafe(index);

        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();

        DocumentMapper defaultMapper = parser.parse(type, new CompressedXContent(mapping));
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        defaultMapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String rebuildMapping = builder.string();
        return parser.parse(type, new CompressedXContent(rebuildMapping));
    }

    @Test
    public void testSimpleArrayMapping() throws Exception {
        // @formatter:off
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject(TYPE)
                    .startObject("properties")
                        .startObject("array_field")
                            .field("type", ArrayMapper.CONTENT_TYPE)
                            .startObject(ArrayMapper.INNER_TYPE)
                                .field("type", "keyword")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .string();
        // @formatter:on
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);

        assertThat(mapper.mappers().getMapper("array_field"), is(instanceOf(ArrayMapper.class)));

        BytesReference bytesReference = JsonXContent.contentBuilder()
            .startObject()
            .array("array_field", "a", "b", "c")
            .endObject()
            .bytes();
        SourceToParse sourceToParse = SourceToParse.source(INDEX, TYPE, "abc", bytesReference, XContentType.JSON);
        ParsedDocument doc = mapper.parse(sourceToParse);
        assertThat(doc.dynamicMappingsUpdate() == null, is(true));
        assertThat(doc.docs().size(), is(1));

        ParseContext.Document fields = doc.docs().get(0);
        Set<String> values = uniqueValuesFromFields(fields, "array_field");
        assertThat(values, Matchers.containsInAnyOrder("a", "b", "c"));
        assertThat(
            mapper.mappingSource().string(),
            is("{\"type\":{" +
               "\"properties\":{" +
               "\"array_field\":{" +
               "\"type\":\"array\"," +
               "\"inner\":{" +
               "\"type\":\"keyword\"" +
               "}" +
               "}" +
               "}" +
               "}}"));
    }

    private static Set<String> uniqueValuesFromFields(ParseContext.Document fields, String fieldName) {
        return Stream.of(fields.getFields(fieldName))
                .map(IndexableField::binaryValue)
                .map(BytesRef::utf8ToString)
                .collect(Collectors.toSet());
    }

    @Test
    public void testInvalidArraySimpleField() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject().startObject("type").startObject("properties")
            .startObject("array_field")
            .field("type", ArrayMapper.CONTENT_TYPE)
            .startObject(ArrayMapper.INNER_TYPE)
            .field("type", "string")
            .endObject()
            .endObject()
            .endObject().endObject().endObject()
            .string();

        expectedException.expect(MapperParsingException.class);
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);
    }

    @Test
    public void testInvalidArrayNonConvertableType() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject().startObject("type").startObject("properties")
            .startObject("array_field")
            .field("type", ArrayMapper.CONTENT_TYPE)
            .startObject(ArrayMapper.INNER_TYPE)
            .field("type", "double")
            .endObject()
            .endObject()
            .endObject().endObject().endObject()
            .string();
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);

        expectedException.expect(MapperParsingException.class);
        expectedException.expectMessage("failed to parse [array_field]");

        BytesReference bytesReference = XContentFactory.jsonBuilder()
            .startObject()
            .array("array_field", true, false, true)
            .endObject()
            .bytes();
        SourceToParse sourceToParse = SourceToParse.source(INDEX, TYPE, "abc", bytesReference, XContentType.JSON);
        mapper.parse(sourceToParse);
    }

    @Test
    public void testObjectArrayMapping() throws Exception {
        // @formatter: off
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("array_field")
                            .field("type", ArrayMapper.CONTENT_TYPE)
                            .startObject(ArrayMapper.INNER_TYPE)
                                .field("type", "object")
                                .field("dynamic", true)
                                .startObject("properties")
                                    .startObject("s")
                                        .field("type", "keyword")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .string();
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);
        // child object mapper
        assertThat(mapper.objectMappers().get("array_field"), is(instanceOf(ObjectArrayMapper.class)));
        BytesReference bytesReference = XContentFactory.jsonBuilder()
            .startObject()
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
            .bytes();
        SourceToParse sourceToParse = SourceToParse.source(INDEX, TYPE, "abc", bytesReference, XContentType.JSON);
        ParsedDocument doc = mapper.parse(sourceToParse);
        // @formatter: off
        assertThat(doc.dynamicMappingsUpdate(), nullValue());
        assertThat(doc.docs().size(), is(1));
        assertThat(
            uniqueValuesFromFields(doc.docs().get(0), "array_field.s"),
            containsInAnyOrder("a", "b", "c"));
        assertThat(mapper.mappers().smartNameFieldMapper("array_field.s"), instanceOf(KeywordFieldMapper.class));
        assertThat(
            mapper.mappingSource().string(),
            is("{\"type\":{" +
               "\"properties\":{" +
               "\"array_field\":{" +
               "\"type\":\"array\"," +
               "\"inner\":{" +
               "\"dynamic\":\"true\"," +
               "\"properties\":{" +
               "\"s\":{" +
               "\"type\":\"keyword\"" +
               "}" +
               "}" +
               "}" +
               "}" +
               "}}}"));
    }

    @Test
    public void testObjectArrayMappingNewColumn() throws Exception {
        // @formatter: off
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("array_field")
                            .field("type", ArrayMapper.CONTENT_TYPE)
                            .startObject(ArrayMapper.INNER_TYPE)
                                .field("type", "object")
                                .field("dynamic", true)
                                .startObject("properties")
                                    .startObject("s")
                                        .field("type", "keyword")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .string();
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);
        // child object mapper
        assertThat(mapper.objectMappers().get("array_field"), is(instanceOf(ObjectArrayMapper.class)));
        BytesReference bytesReference = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("array_field")
            .startObject()
            .field("s", "a")
            .field("new", true)
            .endObject()
            .endArray()
            .endObject()
            .bytes();
        SourceToParse sourceToParse = SourceToParse.source(INDEX, TYPE, "abc", bytesReference, XContentType.JSON);
        ParsedDocument doc = mapper.parse(sourceToParse);

        Mapping mappingUpdate = doc.dynamicMappingsUpdate();
        assertThat(mappingUpdate, notNullValue());
        mapper = mapper.merge(mappingUpdate, true);
        assertThat(doc.docs().size(), is(1));
        String[] values = doc.docs().get(0).getValues("array_field.new");
        assertThat(values, arrayContainingInAnyOrder(is("T"), is("1")));
        String mappingSourceString = new CompressedXContent(mapper, XContentType.JSON, ToXContent.EMPTY_PARAMS).string();
        assertThat(
            mappingSourceString,
            is("{\"type\":{" +
               "\"properties\":{" +
               "\"array_field\":{" +
               "\"type\":\"array\"," +
               "\"inner\":{" +
               "\"dynamic\":\"true\"," +
               "\"properties\":{" +
               "\"new\":{\"type\":\"boolean\"}," +
               "\"s\":{" +
               "\"type\":\"keyword\"" +
               "}" +
               "}" +
               "}" +
               "}" +
               "}}}"));
    }

    @Test
    public void testInsertGetArray() throws Exception {
        // @formatter:off
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("array_field")
                            .field("type", ArrayMapper.CONTENT_TYPE)
                            .startObject(ArrayMapper.INNER_TYPE)
                                .field("type", "double")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .string();
        // @formatter:on
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);
        IndexResponse response = client()
            .prepareIndex(INDEX, "type")
            .setId("123")
            .setSource("{\"array_field\":[0.0, 99.9, -100.5678]}", XContentType.JSON)
            .execute().actionGet();
        assertThat(response.getVersion(), is(1L));

        client().admin().indices().prepareRefresh(INDEX).execute().actionGet();

        // realtime
        GetResponse rtGetResponse = client().prepareGet(INDEX, "type", "123")
            .setFetchSource(true)
            .setStoredFields("array_field").setRealtime(true).execute().actionGet();
        assertThat(rtGetResponse.getId(), is("123"));
        assertThat(Joiner.on(',').withKeyValueSeparator(":").join(rtGetResponse.getSource()), is("array_field:[0.0, 99.9, -100.5678]"));

        // non-realtime
        GetResponse getResponse = client().prepareGet(INDEX, "type", "123")
            .setFetchSource(true).setStoredFields("array_field").setRealtime(false).execute().actionGet();
        assertThat(getResponse.getId(), is("123"));
        assertThat(Joiner.on(',').withKeyValueSeparator(":").join(getResponse.getSource()), is("array_field:[0.0, 99.9, -100.5678]"));
    }

    @Test
    public void testInsertSearchArray() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject().startObject("type").startObject("properties")
            .startObject("array_field")
            .field("type", ArrayMapper.CONTENT_TYPE)
            .startObject(ArrayMapper.INNER_TYPE)
            .field("type", "double")
            .endObject()
            .endObject()
            .endObject().endObject().endObject()
            .string();
        mapper(INDEX, TYPE, mapping);
        IndexResponse response = client()
            .prepareIndex(INDEX, "type")
            .setId("123")
            .setSource("{\"array_field\":[0.0, 99.9, -100.5678]}", XContentType.JSON)
            .execute().actionGet();
        assertThat(response.getVersion(), is(1L));

        client().admin().indices().prepareRefresh(INDEX).execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch(INDEX).setTypes("type")
            .setFetchSource(true)
            .setQuery(QueryBuilders.termQuery("array_field", 0.0d)).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), is(1L));
        assertThat(Joiner.on(',').withKeyValueSeparator(":").join(
            searchResponse.getHits().getAt(0).getSourceAsMap()),
            is("array_field:[0.0, 99.9, -100.5678]"));

        Object values = searchResponse.getHits().getAt(0).getSourceAsMap().get("array_field");
        assertThat((List<Double>) values, Matchers.hasItems(0.0D, 99.9D, -100.5678D));
    }

    @Test
    public void testEmptyArray() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject().startObject("type").startObject("properties")
            .startObject("array_field")
            .field("type", ArrayMapper.CONTENT_TYPE)
            .startObject(ArrayMapper.INNER_TYPE)
            .field("type", "double")
            .endObject()
            .endObject()
            .endObject().endObject().endObject()
            .string();
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);

        // parse source with empty array
        BytesReference bytesReference = XContentFactory.jsonBuilder()
            .startObject()
            .array("array_field")
            .endObject()
            .bytes();
        SourceToParse sourceToParse = SourceToParse.source(INDEX, TYPE, "abc", bytesReference, XContentType.JSON);
        ParsedDocument doc = mapper.parse(sourceToParse);
        assertThat(doc.dynamicMappingsUpdate() == null, is(true));
        assertThat(doc.docs().size(), is(1));
        assertThat(doc.docs().get(0).get("array_field"), is(nullValue())); // no lucene field generated

        // insert
        IndexResponse response = client()
            .prepareIndex(INDEX, "type", "123")
            .setSource("{\"array_field\":[]}", XContentType.JSON)
            .execute().actionGet();
        assertThat(response.getVersion(), is(1L));

        client().admin().indices().prepareRefresh(INDEX).execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch(INDEX).setTypes("type")
            .setFetchSource(true).addStoredField("array_field")
            .setQuery(new TermsQueryBuilder("_id", new int[]{123}))
            .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), is(1L));
        assertThat(Joiner.on(',').withKeyValueSeparator(":").join(
            searchResponse.getHits().getAt(0).getSourceAsMap()),
            is("array_field:[]"));
        assertThat(searchResponse.getHits().getAt(0).getFields().containsKey("array_field"), is(false));
    }

    @Test
    public void testNestedArrayMapping() throws Exception {
        expectedException.expect(MapperParsingException.class);
        expectedException.expectMessage("nested arrays are not supported");

        String mapping = XContentFactory.jsonBuilder()
            .startObject().startObject("type").startObject("properties")
            .startObject("array_field")
            .field("type", ArrayMapper.CONTENT_TYPE)
            .startObject(ArrayMapper.INNER_TYPE)
            .field("type", ArrayMapper.CONTENT_TYPE)
            .startObject(ArrayMapper.INNER_TYPE)
            .field("type", "double")
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
            .startObject(ArrayMapper.INNER_TYPE)
            .field("type", "double")
            .endObject()
            .endObject()
            .endObject().endObject().endObject()
            .string();
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);

        expectedException.expect(MapperParsingException.class);
        expectedException.expectMessage("failed to parse");
        expectedException.expectCause(CauseMatcher.cause(ElasticsearchParseException.class, "nested arrays are not supported"));
        // parse source with empty array
        BytesReference bytesReference = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("new_array_field")
            .startArray().value("a").value("b").endArray()
            .endArray()
            .endObject()
            .bytes();
        SourceToParse sourceToParse = SourceToParse.source(INDEX, TYPE, "abc", bytesReference, XContentType.JSON);
        mapper.parse(sourceToParse);

    }

    @Test
    public void testParseNull() throws Exception {
        // @formatter: off
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("array_field")
                            .field("type", ArrayMapper.CONTENT_TYPE)
                            .startObject(ArrayMapper.INNER_TYPE)
                                .field("type", "double")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .string();
        // @formatter: on
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);
        BytesReference bytesReference = XContentFactory.jsonBuilder()
            .startObject()
            .nullField("array_field")
            .endObject()
            .bytes();
        SourceToParse sourceToParse = SourceToParse.source(INDEX, TYPE, "abc", bytesReference, XContentType.JSON);
        ParsedDocument parsedDoc = mapper.parse(sourceToParse);
        assertThat(parsedDoc.docs().size(), is(1));
        assertThat(parsedDoc.docs().get(0).getField("array_field"), is(nullValue()));
    }

    @Test
    public void testParseNullOnObjectArray() throws Exception {
        // @formatter: on
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("array_field")
                            .field("type", ArrayMapper.CONTENT_TYPE)
                            .startObject(ArrayMapper.INNER_TYPE)
                                .field("type", "object")
                                .startObject("properties")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .string();
        // @formatter: off
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);
        BytesReference bytesReference = XContentFactory.jsonBuilder()
            .startObject()
            .nullField("array_field")
            .endObject()
            .bytes();
        SourceToParse sourceToParse = SourceToParse.source(INDEX, TYPE, "abc", bytesReference, XContentType.JSON);
        ParsedDocument parsedDoc = mapper.parse(sourceToParse);
        assertThat(parsedDoc.docs().size(), is(1));
        assertThat(parsedDoc.docs().get(0).getField("array_field"), is(nullValue()));


    }

    @Test
    public void testParseDynamicEmptyArray() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject().startObject("type").startObject("properties")
            .endObject().endObject().endObject()
            .string();
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);

        // parse source with empty array
        BytesReference bytesReference = XContentFactory.jsonBuilder()
            .startObject()
            .array("new_array_field")
            .endObject()
            .bytes();
        SourceToParse sourceToParse = SourceToParse.source(INDEX, TYPE, "abc", bytesReference, XContentType.JSON);
        ParsedDocument doc = mapper.parse(sourceToParse);
        assertThat(doc.docs().get(0).getField("new_array_field"), is(nullValue()));
        assertThat(mapper.mappers().smartNameFieldMapper("new_array_field"), is(nullValue()));
    }

    @Test
    public void testParseDynamicNullArray() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject().startObject("type").startObject("properties")
            .endObject().endObject().endObject()
            .string();
        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);

        // parse source with null array
        BytesReference bytesReference = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("new_array_field").nullValue().endArray()
            .endObject()
            .bytes();
        SourceToParse sourceToParse = SourceToParse.source(INDEX, TYPE, "abc", bytesReference, XContentType.JSON);
        ParsedDocument doc = mapper.parse(sourceToParse);
        assertThat(doc.docs().get(0).getField("new_array_field"), is(nullValue()));
        assertThat(mapper.mappers().smartNameFieldMapper("new_array_field"), is(nullValue()));
    }

    @Test
    public void testCopyToFieldsOfInnerMapping() throws Exception {
        // @formatter:off
        String mapping = XContentFactory.jsonBuilder()
            .startObject().startObject("type").startObject("properties")
                .startObject("string_array")
                    .field("type", ArrayMapper.CONTENT_TYPE)
                    .startObject(ArrayMapper.INNER_TYPE)
                        .field("type", "text")
                        .field("index", "true")
                        .field("copy_to", "string_array_ft")
                    .endObject()
                .endObject()
            .endObject().endObject().endObject()
            .string();
        // @formatter:on

        DocumentMapper mapper = mapper(INDEX, TYPE, mapping);
        FieldMapper arrayMapper = mapper.mappers().getMapper("string_array");
        assertThat(arrayMapper, is(instanceOf(ArrayMapper.class)));
        assertThat(arrayMapper.copyTo().copyToFields(), contains("string_array_ft"));

        // @formatter:on
        BytesReference bytesReference = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("string_array")
            .value("foo")
            .value("bar")
            .endArray()
            .endObject()
            .bytes();
        SourceToParse sourceToParse = SourceToParse.source(INDEX, TYPE, "1", bytesReference, XContentType.JSON);
        ParsedDocument doc = mapper.parse(sourceToParse);
        // @formatter:off

        List<String> copyValues = new ArrayList<>();
        for (IndexableField field : doc.docs().get(0).getFields()) {
            if (field.name().equals("string_array_ft")) {
                copyValues.add(field.stringValue());
            }
        }
        assertThat(copyValues, containsInAnyOrder("foo", "bar"));
    }
}
