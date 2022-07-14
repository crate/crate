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

package io.crate.metadata.doc.mappers;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.indices.IndicesModule;
import org.junit.Test;

import io.crate.Constants;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class KeywordFieldMapperTest extends CrateDummyClusterServiceUnitTest {

    private static final String TYPE = Constants.DEFAULT_MAPPING_TYPE;

    private static DocumentMapperParser parser(Mapper.TypeParser parser) throws IOException {
        MapperService mapperService = MapperTestUtils.newMapperService(
            NamedXContentRegistry.EMPTY,
            createTempDir(),
            Settings.EMPTY,
            new IndicesModule(List.of()),
            ""
        );
        return mapperService.documentMapperParser();
    }

    @Test
    public void test_keyword_field_parser_on_mapping_with_length_limit() throws Exception {
        String expectedMapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(TYPE)
                        .startObject("properties")
                            .startObject("text_col")
                                .field("type", "keyword")
                                .field("index", false)
                                .field("position", 1)
                                .field("length_limit", 1)
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject());

        var parser = parser(new KeywordFieldMapper.TypeParser());
        // string -> doXContentBody -> parse -> string
        var compressedXContent = new CompressedXContent(expectedMapping);
        var mapper = parser.parse(compressedXContent);
        var actualMapping = mapper.mappingSource().toString();

        assertThat(expectedMapping, is(actualMapping));
        assertThat(actualMapping, is(
            "{\"default\":{\"properties\":" +
            "{\"text_col\":{\"type\":\"keyword\",\"index\":false,\"position\":1,\"length_limit\":1}}}}"));
    }

    @Test
    public void test_keywords_fields_mapping_merge_fail_on_different_length_limit() throws Exception {
        var parser = parser(new TextFieldMapper.TypeParser());

        var mapper = parser.parse(new CompressedXContent(Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(TYPE)
                        .startObject("properties")
                            .startObject("text_col")
                                .field("type", "keyword")
                                .field("position", 1)
                                .field("length_limit", 2)
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject())));
        var anotherMapper = parser.parse(new CompressedXContent(Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(TYPE)
                        .startObject("properties")
                            .startObject("text_col")
                                .field("type", "keyword")
                                .field("index", false)
                                .field("position", 2)
                                .field("length_limit", 1)
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject())));

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("mapper [text_col] has different length_limit settings, current [2], merged [1]");
        mapper.merge(anotherMapper.mapping());
    }
}
