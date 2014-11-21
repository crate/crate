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

package org.elasticsearch.index.mapper.core;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.array.ArrayValueMapperParser;
import org.elasticsearch.index.mapper.array.DynamicArrayFieldMapperBuilderFactory;
import org.elasticsearch.index.mapper.object.DynamicValueMapperLookup;
import org.elasticsearch.index.mapper.object.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * fieldmapper for encoding and handling arrays explicitly
 *
 * handler for type "array".
 *
 * accepts mappings like:
 *
 *  "array_field": {
 *      "type": "array",
 *      "inner": {
 *          "type": "boolean",
 *          "null_value": true
 *      }
 *  }
 *
 *  This would be parsed as a array of booleans.
 *  This field now only accepts arrays, no single values.
 *  So inserting a document like:
 *
 *  {
 *      "array_field": true
 *  }
 *
 *  will fail, while a document like:
 *
 *  {
 *      "array_field": [true]
 *  }
 *
 *  will pass.
 *
 */
public class ArrayMapper implements ArrayValueMapperParser, Mapper {

    public static final String CONTENT_TYPE = "array";
    public static final XContentBuilderString INNER = new XContentBuilderString("inner");
    private Mapper innerMapper;

    public static class Builder extends Mapper.Builder<Builder, ArrayMapper> {

        private Mapper.Builder innerMapperBuilder;

        public static class BuilderFactory implements DynamicArrayFieldMapperBuilderFactory {

            public Builder create(String name){
                return new Builder(name);
            }
        }

        public Builder(String name) {
            super(name);
        }

        public Builder innerMapperBuilder(Mapper.Builder builder) {
            this.innerMapperBuilder = builder;
            return this;
        }


        @Override
        public ArrayMapper build(BuilderContext context) {
            if (innerMapperBuilder == null) {
                return new ArrayMapper(name);
            } else {
                Mapper innerMapper = innerMapperBuilder.build(context);
                return new ArrayMapper(innerMapper, name);
            }
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            boolean innerFound = false;
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("inner")) {
                    innerFound = true;
                    if (fieldNode == null || !(fieldNode instanceof Map)) {
                        throw new MapperParsingException("property [inner] must be a map");
                    }
                    Map<String, Object> innerNode = (Map<String, Object>)fieldNode;
                    String typeName = (String)innerNode.get("type");
                    if (typeName == null && innerNode.containsKey("properties")) {
                        typeName = ObjectMapper.CONTENT_TYPE;
                    }
                    Mapper.TypeParser innerTypeParser = parserContext.typeParser(typeName);
                    builder.innerMapperBuilder(innerTypeParser.parse(name, innerNode, parserContext));
                }
            }
            if (!innerFound) {
                throw new MapperParsingException("property [inner] missing");
            }
            return builder;
        }
    }

    private final String name;

    private ArrayMapper(String name) {
        this.name = name;
    }

    public ArrayMapper(Mapper innerMapper, String name) {
        this.innerMapper = innerMapper;
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_ARRAY) {
            throw new ElasticsearchParseException("invalid array");
        }
        token = parser.nextToken();
        if (innerMapper == null) {
            Mapper mapper = DynamicValueMapperLookup.getMapper(context, name(), token);
            if (mapper == null) {
                return;
            }
            innerMapper = mapper;
        }
        while (token != XContentParser.Token.END_ARRAY) {
            // we only get here for non-empty arrays
            if (innerMapper instanceof ObjectMapper) {
                context.path().add(name());
            }
            innerMapper.parse(context);
            token = parser.nextToken();
        }
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        innerMapper.merge(mergeWith, mergeContext);
    }

    @Override
    public void traverse(FieldMapperListener fieldMapperListener) {
        innerMapper.traverse(fieldMapperListener);
    }

    @Override
    public void traverse(ObjectMapperListener objectMapperListener) {
        innerMapper.traverse(objectMapperListener);
    }

    @Override
    public void close() {
        innerMapper.close();
    }

    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(innerMapper.name());
        builder.field("type", contentType());
        builder.startObject(INNER);
        if (innerMapper instanceof AbstractFieldMapper) {
            boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
            ((AbstractFieldMapper)innerMapper).doXContentBody(builder, includeDefaults, params);
        } else if (innerMapper instanceof ObjectMapper) {
            ((ObjectMapper)innerMapper).doXContentBody(builder, params, null, Mapper.EMPTY_ARRAY);
        }

        builder.endObject();
        return builder.endObject();
    }
}
