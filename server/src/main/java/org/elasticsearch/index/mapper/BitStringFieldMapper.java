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

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.common.xcontent.XContentBuilder;


public class BitStringFieldMapper extends FieldMapper {

    private final Integer length;

    protected BitStringFieldMapper(String simpleName,
                                   int position,
                                   long columnOID,
                                   boolean isDropped,
                                   Integer length,
                                   String defaultExpression,
                                   FieldType fieldType,
                                   MappedFieldType mappedFieldType,
                                   CopyTo copyTo) {
        super(
            simpleName,
            position,
            columnOID,
            isDropped,
            defaultExpression,
            fieldType,
            mappedFieldType,
            copyTo
        );
        this.length = length;
    }

    public static final String CONTENT_TYPE = "bit";

    public static final class Defaults {

        private Defaults() {}

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.freeze();
        }
    }

    static class BitStringFieldType extends MappedFieldType {

        BitStringFieldType(String name, boolean isSearchable, boolean hasDocValues) {
            super(name, isSearchable, hasDocValues);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }
    }

    public static class Builder extends FieldMapper.Builder {

        private Integer length;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
        }

        @Override
        public BitStringFieldMapper build(BuilderContext context) {
            var mapper = new BitStringFieldMapper(
                name,
                position,
                columnOID,
                isDropped,
                length,
                defaultExpression,
                fieldType,
                new BitStringFieldType(buildFullName(context), indexed, hasDocValues),
                copyTo);
            context.putPositionInfo(mapper, position);
            return mapper;
        }

        public void length(Integer length) {
            this.length = length;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public org.elasticsearch.index.mapper.Mapper.Builder parse(
                String name,
                Map<String, Object> node,
                ParserContext parserContext) throws MapperParsingException {

            Builder builder = new Builder(name);
            TypeParsers.parseField(builder, name, node);
            Object length = node.remove("length");
            assert length != null : "length property is required for `bit` type";
            builder.length((Integer) length);
            return builder;
        }
    }

    @Override
    protected BitStringFieldMapper clone() {
        return (BitStringFieldMapper) super.clone();
    }

    @Override
    public BitStringFieldType fieldType() {
        return (BitStringFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults) throws IOException {
        super.doXContentBody(builder, includeDefaults);
        builder.field("length", length);
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        BitStringFieldMapper o = (BitStringFieldMapper) other;
        if (length != o.length) {
            conflicts.add("mapper [" + name() + "] has different [length] values");
        }
    }
}
