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

package org.elasticsearch.index.mapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorEncoding;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.FloatArrayList;

import io.crate.execution.dml.FloatVectorIndexer;
import io.crate.types.FloatVectorType;

public class FloatVectorFieldMapper extends FieldMapper implements ArrayValueMapperParser {

    public static final class Defaults {

        private Defaults() {}

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.freeze();
        }
    }

    static class VectorFieldType extends MappedFieldType {

        public VectorFieldType(String name, boolean isIndexed, boolean hasDocValues) {
            super(name, isIndexed, hasDocValues, false);
        }

        @Override
        public String typeName() {
            return FloatVectorType.INSTANCE_ONE.getName();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        private int dimensions = 0;

        protected Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
        }

        @Override
        public Mapper build(BuilderContext context) {
            fieldType.setVectorAttributes(
                dimensions,
                VectorEncoding.FLOAT32,
                FloatVectorType.SIMILARITY_FUNC
            );
            var mapper = new FloatVectorFieldMapper(
                name,
                position,
                defaultExpression,
                fieldType,
                new VectorFieldType(buildFullName(context), indexed, hasDocValues),
                copyTo
            );
            context.putPositionInfo(mapper, position);
            return mapper;
        }

        public void dimensions(int dimensions) {
            this.dimensions = dimensions;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?> parse(String name,
                                       Map<String, Object> node,
                                       ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            TypeParsers.parseField(builder, name, node);
            builder.dimensions((Integer) node.remove("dimensions"));
            return builder;
        }
    }

    protected FloatVectorFieldMapper(String simpleName,
                                int position,
                                @Nullable String defaultExpression,
                                FieldType fieldType,
                                MappedFieldType mappedFieldType,
                                CopyTo copyTo) {
        super(simpleName, position, defaultExpression, fieldType, mappedFieldType, copyTo);
    }

    @Override
    protected void parseCreateField(ParseContext context, Consumer<IndexableField> onField) throws IOException {
        XContentParser.Token token = context.parser().currentToken();
        if (token == Token.VALUE_NULL) {
            return;
        }
        FloatArrayList vector = new FloatArrayList();
        if (token == XContentParser.Token.START_ARRAY) {
            token = context.parser().nextToken();
            while (token != XContentParser.Token.END_ARRAY) {
                float value = context.parser().floatValue();
                vector.add(value);
                token = context.parser().nextToken();
            }
        }
        FloatVectorIndexer.createFields(
            fieldType().name(),
            fieldType,
            fieldType().isSearchable(),
            fieldType().hasDocValues(),
            vector.toArray(),
            onField
        );
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        FloatVectorFieldMapper o = (FloatVectorFieldMapper) other;
        if (fieldType.vectorDimension() != o.fieldType.vectorDimension()) {
            conflicts.add("mapper [" + name() + "] has different [dimensions] values");
        }
    }

    @Override
    protected String contentType() {
        return FloatVectorType.INSTANCE_ONE.getName();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults) throws IOException {
        super.doXContentBody(builder, includeDefaults);
        builder.field("dimensions", fieldType.vectorDimension());
    }
}
