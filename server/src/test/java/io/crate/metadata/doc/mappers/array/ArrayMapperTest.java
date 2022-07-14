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

package io.crate.metadata.doc.mappers.array;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.ArrayMapper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ObjectArrayMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.indices.IndicesModule;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.Constants;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class ArrayMapperTest extends CrateDummyClusterServiceUnitTest {

    public static final String INDEX = "my_index";
    public static final String TYPE = Constants.DEFAULT_MAPPING_TYPE;

    /**
     * create index with type and mapping and validate DocumentMapper serialization
     */
    private DocumentMapper mapper(String indexName, String mapping) throws IOException {
        IndicesModule indicesModule = new IndicesModule(List.of());
        MapperService mapperService = MapperTestUtils.newMapperService(
            NamedXContentRegistry.EMPTY,
            createTempDir(),
            Settings.EMPTY,
            indicesModule,
            indexName
        );
        DocumentMapperParser parser = mapperService.documentMapperParser();

        DocumentMapper defaultMapper = parser.parse(new CompressedXContent(mapping));
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        defaultMapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String rebuildMapping = Strings.toString(builder);
        return parser.parse(new CompressedXContent(rebuildMapping));
    }

    @Test
    public void testSimpleArrayMapping() throws Exception {
        // @formatter:off
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject(TYPE)
                    .startObject("properties")
                        .startObject("array_field")
                            .field("type", ArrayMapper.CONTENT_TYPE)
                            .startObject(ArrayMapper.INNER_TYPE)
                                .field("type", "keyword").field("position", 1)
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        // @formatter:on
        DocumentMapper mapper = mapper(INDEX, mapping);

        assertThat(mapper.mappers().getMapper("array_field"), is(instanceOf(ArrayMapper.class)));

        BytesReference bytesReference = BytesReference.bytes(JsonXContent.contentBuilder()
            .startObject()
            .array("array_field", "a", "b", "c")
            .endObject());
        SourceToParse sourceToParse = new SourceToParse(INDEX, "abc", bytesReference, XContentType.JSON);
        ParsedDocument doc = mapper.parse(sourceToParse);
        assertThat(doc.dynamicMappingsUpdate() == null, is(true));
        assertThat(doc.docs().size(), is(1));

        ParseContext.Document fields = doc.docs().get(0);
        Set<String> values = uniqueValuesFromFields(fields, "array_field");
        assertThat(values, Matchers.containsInAnyOrder("a", "b", "c"));
        assertThat(
            mapper.mappingSource().string(),
            is("{\"default\":{" +
               "\"properties\":{" +
               "\"array_field\":{" +
               "\"type\":\"array\"," +
               "\"inner\":{" +
               "\"type\":\"keyword\"," +
               "\"position\":1" +
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
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("type").startObject("properties")
            .startObject("array_field")
            .field("type", ArrayMapper.CONTENT_TYPE)
            .startObject(ArrayMapper.INNER_TYPE)
            .field("type", "string")
            .endObject()
            .endObject()
            .endObject().endObject().endObject());

        expectedException.expect(MapperParsingException.class);
        mapper(INDEX, mapping);
    }

    @Test
    public void testInvalidArrayNonConvertableType() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject(TYPE).startObject("properties")
            .startObject("array_field")
            .field("type", ArrayMapper.CONTENT_TYPE)
            .startObject(ArrayMapper.INNER_TYPE)
            .field("type", "double").field("position", 1)
            .endObject()
            .endObject()
            .endObject().endObject().endObject());
        DocumentMapper mapper = mapper(INDEX, mapping);

        expectedException.expect(MapperParsingException.class);
        expectedException.expectMessage("failed to parse field [array_field] of type [double]");

        BytesReference bytesReference = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .array("array_field", true, false, true)
            .endObject());
        SourceToParse sourceToParse = new SourceToParse(INDEX, "abc", bytesReference, XContentType.JSON);
        mapper.parse(sourceToParse);
    }

    @Test
    public void testObjectArrayMapping() throws Exception {
        // @formatter: off
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject(TYPE)
                    .startObject("properties")
                        .startObject("array_field")
                            .field("type", ArrayMapper.CONTENT_TYPE)
                            .startObject(ArrayMapper.INNER_TYPE)
                                .field("type", "object").field("position", 1)
                                .field("dynamic", true)
                                .startObject("properties")
                                    .startObject("s")
                                        .field("type", "keyword").field("position", 2)
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        DocumentMapper mapper = mapper(INDEX, mapping);
        // child object mapper
        assertThat(mapper.objectMappers().get("array_field"), is(instanceOf(ObjectArrayMapper.class)));
        BytesReference bytesReference = BytesReference.bytes(XContentFactory.jsonBuilder()
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
            .endObject());
        SourceToParse sourceToParse = new SourceToParse(INDEX, "abc", bytesReference, XContentType.JSON);
        ParsedDocument doc = mapper.parse(sourceToParse);
        // @formatter: off
        assertThat(doc.dynamicMappingsUpdate(), nullValue());
        assertThat(doc.docs().size(), is(1));
        assertThat(
            uniqueValuesFromFields(doc.docs().get(0), "array_field.s"),
            containsInAnyOrder("a", "b", "c"));
        assertThat(mapper.mappers().getMapper("array_field.s"), instanceOf(KeywordFieldMapper.class));
        assertThat(
            mapper.mappingSource().string(),
            is("{\"default\":{" +
               "\"properties\":{" +
               "\"array_field\":{" +
               "\"type\":\"array\"," +
               "\"inner\":{" +
               "\"position\":1," +
               "\"dynamic\":\"true\"," +
               "\"properties\":{" +
               "\"s\":{" +
               "\"type\":\"keyword\"," +
               "\"position\":2" +
               "}" +
               "}" +
               "}" +
               "}" +
               "}}}"));
    }

    @Test
    public void testObjectArrayMappingNewColumn() throws Exception {
        // @formatter: off
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject(TYPE)
                    .startObject("properties")
                        .startObject("array_field")
                            .field("type", ArrayMapper.CONTENT_TYPE)
                            .startObject(ArrayMapper.INNER_TYPE)
                                .field("type", "object")
                                .field("dynamic", true)
                                .field("position", 1)
                                .startObject("properties")
                                    .startObject("s")
                                        .field("type", "keyword")
                                        .field("position", 2)
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        DocumentMapper mapper = mapper(INDEX, mapping);
        // child object mapper
        assertThat(mapper.objectMappers().get("array_field"), is(instanceOf(ObjectArrayMapper.class)));
        BytesReference bytesReference = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .startArray("array_field")
            .startObject()
            .field("s", "a")
            .field("new", true)
            .endObject()
            .endArray()
            .endObject());
        SourceToParse sourceToParse = new SourceToParse(INDEX, "abc", bytesReference, XContentType.JSON);
        ParsedDocument doc = mapper.parse(sourceToParse);

        Mapping mappingUpdate = doc.dynamicMappingsUpdate();
        assertThat(mappingUpdate, notNullValue());
        mapper = mapper.merge(mappingUpdate);
        assertThat(doc.docs().size(), is(1));
        String[] values = doc.docs().get(0).getValues("array_field.new");
        assertThat(values, arrayContainingInAnyOrder(is("T"), is("1")));
        String mappingSourceString = new CompressedXContent(mapper, XContentType.JSON, ToXContent.EMPTY_PARAMS).string();
        // column position calculation is carried out within clusterstate changes, so 'new' still has position = null
        assertThat(
            mappingSourceString,
            is("{\"default\":{" +
               "\"properties\":{" +
               "\"array_field\":{" +
               "\"type\":\"array\"," +
               "\"inner\":{" +
               "\"position\":1," +
               "\"dynamic\":\"true\"," +
               "\"properties\":{" +
               "\"new\":{\"type\":\"boolean\",\"position\":-1}," +
               "\"s\":{" +
               "\"type\":\"keyword\"," +
               "\"position\":2" +
               "}" +
               "}" +
               "}" +
               "}" +
               "}}}"));
    }

    @Test
    public void testNestedArrayMapping() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject(TYPE).startObject("properties")
            .startObject("array_field")
            .field("type", ArrayMapper.CONTENT_TYPE)
            .startObject(ArrayMapper.INNER_TYPE)
            .field("type", ArrayMapper.CONTENT_TYPE)
            .startObject(ArrayMapper.INNER_TYPE)
            .field("type", "double")
            .endObject()
            .endObject()
            .endObject()
            .endObject().endObject().endObject());

        expectedException.expect(MapperParsingException.class);
        expectedException.expectMessage("nested arrays are not supported");
        mapper(INDEX, mapping);
    }

    @Test
    public void testParseNull() throws Exception {
        // @formatter: off
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject(TYPE)
                    .startObject("properties")
                        .startObject("array_field")
                            .field("type", ArrayMapper.CONTENT_TYPE)
                            .startObject(ArrayMapper.INNER_TYPE)
                                .field("type", "double").field("position", 1)
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        // @formatter: on
        DocumentMapper mapper = mapper(INDEX, mapping);
        BytesReference bytesReference = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .nullField("array_field")
            .endObject());
        SourceToParse sourceToParse = new SourceToParse(INDEX, "abc", bytesReference, XContentType.JSON);
        ParsedDocument parsedDoc = mapper.parse(sourceToParse);
        assertThat(parsedDoc.docs().size(), is(1));
        assertThat(parsedDoc.docs().get(0).getField("array_field"), is(nullValue()));
    }

    @Test
    public void testParseNullOnObjectArray() throws Exception {
        // @formatter: on
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject(TYPE)
                    .startObject("properties")
                        .startObject("array_field")
                            .field("type", ArrayMapper.CONTENT_TYPE)
                            .startObject(ArrayMapper.INNER_TYPE)
                                .field("type", "object").field("position", 1)
                                .startObject("properties")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        // @formatter: off
        DocumentMapper mapper = mapper(INDEX, mapping);
        BytesReference bytesReference = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .nullField("array_field")
            .endObject());
        SourceToParse sourceToParse = new SourceToParse(INDEX, "abc", bytesReference, XContentType.JSON);
        ParsedDocument parsedDoc = mapper.parse(sourceToParse);
        assertThat(parsedDoc.docs().size(), is(1));
        assertThat(parsedDoc.docs().get(0).getField("array_field"), is(nullValue()));


    }

    @Test
    public void testParseDynamicEmptyArray() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject(TYPE).startObject("properties")
            .endObject().endObject().endObject());
        DocumentMapper mapper = mapper(INDEX, mapping);

        // parse source with empty array
        BytesReference bytesReference = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .array("new_array_field")
            .endObject());
        SourceToParse sourceToParse = new SourceToParse(INDEX, "abc", bytesReference, XContentType.JSON);
        ParsedDocument doc = mapper.parse(sourceToParse);
        assertThat(doc.docs().get(0).getField("new_array_field"), is(nullValue()));
        assertThat(mapper.mappers().getMapper("new_array_field"), is(nullValue()));
    }

    @Test
    public void testParseDynamicNullArray() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject(TYPE).startObject("properties")
            .endObject().endObject().endObject());
        DocumentMapper mapper = mapper(INDEX, mapping);

        // parse source with null array
        BytesReference bytesReference = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .startArray("new_array_field").nullValue().endArray()
            .endObject());
        SourceToParse sourceToParse = new SourceToParse(INDEX, "abc", bytesReference, XContentType.JSON);
        ParsedDocument doc = mapper.parse(sourceToParse);
        assertThat(doc.docs().get(0).getField("new_array_field"), is(nullValue()));
        assertThat(mapper.mappers().getMapper("new_array_field"), is(nullValue()));
    }

    @Test
    public void testCopyToFieldsOfInnerMapping() throws Exception {
        // @formatter:off
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject(TYPE).startObject("properties")
                .startObject("string_array")
                    .field("type", ArrayMapper.CONTENT_TYPE)
                    .startObject(ArrayMapper.INNER_TYPE)
                        .field("type", "text")
                        .field("index", "true")
                        .field("position", 1)
                        .field("copy_to", "string_array_ft")
                    .endObject()
                .endObject()
            .endObject().endObject().endObject());
        // @formatter:on

        DocumentMapper mapper = mapper(INDEX, mapping);
        FieldMapper arrayMapper = (FieldMapper) mapper.mappers().getMapper("string_array");
        assertThat(arrayMapper, is(instanceOf(ArrayMapper.class)));
        assertThat(arrayMapper.copyTo().copyToFields(), contains("string_array_ft"));

        // @formatter:on
        BytesReference bytesReference = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .startArray("string_array")
            .value("foo")
            .value("bar")
            .endArray()
            .endObject());
        SourceToParse sourceToParse = new SourceToParse(INDEX, "1", bytesReference, XContentType.JSON);
        ParsedDocument doc = mapper.parse(sourceToParse);
        // @formatter:off

        List<String> copyValues = new ArrayList<>();
        Document document = doc.docs().get(0);
        IndexableField[] fields = document.getFields("string_array_ft");
        for (var field : fields) {
            if (field.fieldType().docValuesType() == DocValuesType.SORTED_SET) {
                copyValues.add(field.binaryValue().utf8ToString());
            }
        }
        assertThat(copyValues, containsInAnyOrder("foo", "bar"));
    }
}
