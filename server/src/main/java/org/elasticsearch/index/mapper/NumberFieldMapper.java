/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.jetbrains.annotations.Nullable;

/** A {@link FieldMapper} for numeric types: byte, short, int, long, float and double. */
public class NumberFieldMapper extends FieldMapper {

    public static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setStored(false);
        FIELD_TYPE.freeze();
    }

    public static class Builder extends FieldMapper.Builder {

        private final NumberType type;

        public Builder(String name, NumberType type) {
            super(name, FIELD_TYPE);
            this.type = type;
        }

        @Override
        public void indexOptions(IndexOptions indexOptions) {
            throw new MapperParsingException(
                    "index_options not allowed in field [" + name + "] of type [" + type.typeName() + "]");
        }

        @Override
        public NumberFieldMapper build(BuilderContext context) {
            var mapper = new NumberFieldMapper(
                name,
                position,
                columnOID,
                isDropped,
                defaultExpression,
                fieldType,
                new NumberFieldType(buildFullName(context), type, indexed, hasDocValues),
                copyTo
            );
            context.putPositionInfo(mapper, position);
            return mapper;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        final NumberType type;

        public TypeParser(NumberType type) {
            this.type = type;
        }

        @Override
        public Mapper.Builder parse(String name,
                                       Map<String, Object> node,
                                       ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name, type);
            TypeParsers.parseField(builder, name, node);
            return builder;
        }
    }

    public enum NumberType {
        FLOAT("float"),
        DOUBLE("double"),
        BYTE("byte"),
        SHORT("short"),
        INTEGER("integer"),
        LONG("long");

        private final String name;

        NumberType(String name) {
            this.name = name;
        }

        /** Get the associated type name. */
        public final String typeName() {
            return name;
        }
    }

    public static final class NumberFieldType extends MappedFieldType {

        private final NumberType type;

        public NumberFieldType(String name, NumberType type, boolean isSearchable, boolean hasDocValues) {
            super(name, isSearchable, hasDocValues);
            this.type = Objects.requireNonNull(type);
        }

        public NumberFieldType(String name, NumberType type) {
            this(name, type, true, true);
        }

        @Override
        public String typeName() {
            return type.name;
        }

    }

    private NumberFieldMapper(
            String simpleName,
            int position,
            long columnOID,
            boolean isDropped,
            @Nullable String defaultExpression,
            FieldType fieldType,
            MappedFieldType mappedFieldType,
            CopyTo copyTo) {
        super(simpleName, position, columnOID, isDropped, defaultExpression, fieldType, mappedFieldType, copyTo);
    }

    @Override
    public NumberFieldType fieldType() {
        return (NumberFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType().type.typeName();
    }

    @Override
    protected NumberFieldMapper clone() {
        return (NumberFieldMapper) super.clone();
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        NumberFieldMapper m = (NumberFieldMapper) other;
        if (fieldType().type != m.fieldType().type) {
            conflicts.add(
                "mapper [" + name() + "] cannot be changed from type ["
                + fieldType().type.name
                + "] to ["
                + m.fieldType().type.name + "]"
            );
        }
    }
}
