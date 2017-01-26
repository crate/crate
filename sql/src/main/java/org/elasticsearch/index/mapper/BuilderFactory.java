/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package org.elasticsearch.index.mapper;

import com.google.common.base.Throwables;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.array.DynamicArrayFieldMapperBuilderFactory;

import java.io.IOException;

/**
 * Used when a document is parsed and a unknown field that contains an array value is encountered
 *
 * Creates a {@link ArrayMapper} or {@link ObjectArrayMapper}
 */
public class BuilderFactory implements DynamicArrayFieldMapperBuilderFactory {

    public Mapper create(String name, ObjectMapper parentMapper, ParseContext context) {
        Mapper.BuilderContext builderContext = new Mapper.BuilderContext(context.indexSettings(), context.path());
        try {
            Mapper.Builder<?, ?> innerBuilder = detectInnerMapper(context, name, context.parser());
            if (innerBuilder == null) {
                return null;
            }
            Mapper innerMapper = innerBuilder.build(builderContext);
            if (innerMapper instanceof ObjectMapper) {
                ObjectMapper objectMapper = (ObjectMapper) innerMapper;
                return new ObjectArrayMapper(name, objectMapper, context.indexSettings());
            }
            MappedFieldType mappedFieldType = new ArrayFieldType(((FieldMapper.Builder) innerBuilder).fieldType());
            mappedFieldType.setName(name);
            return new ArrayMapper(
                name,
                mappedFieldType,
                mappedFieldType.clone(),
                context.indexSettings(),
                FieldMapper.MultiFields.empty(),
                null,
                innerMapper);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }


    private static Mapper.Builder<?, ?> detectInnerMapper(ParseContext parseContext,
                                                          String fieldName,
                                                          XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            token = parser.nextToken();
        }

        // can't use nulls to detect type
        while (token == XContentParser.Token.VALUE_NULL) {
            token = parser.nextToken();
        }

        if (token == XContentParser.Token.START_ARRAY) {
            throw new ElasticsearchParseException("nested arrays are not supported");
        } else if (token == XContentParser.Token.END_ARRAY) {
            // array is empty or has only null values
            return null;
        }
        return DocumentParser.createBuilderFromDynamicValue(parseContext, token, fieldName);
    }
}
