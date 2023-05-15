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

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.jetbrains.annotations.Nullable;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.XContentParser;

/**
 * A field mapper for boolean fields.
 */
public class BooleanFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "boolean";

    public static final class Defaults {

        private Defaults() {}

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.freeze();
        }
    }

    public static class Values {
        public static final BytesRef TRUE = new BytesRef("T");
        public static final BytesRef FALSE = new BytesRef("F");
    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            this.builder = this;
        }

        @Override
        public BooleanFieldMapper build(BuilderContext context) {
            var mapper = new BooleanFieldMapper(
                name,
                position,
                columnOID,
                defaultExpression,
                fieldType,
                new BooleanFieldType(buildFullName(context), indexed, hasDocValues),
                copyTo);
            context.putPositionInfo(mapper, position);
            return mapper;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<Builder> parse(String name,
                                             Map<String, Object> node,
                                             ParserContext parserContext) throws MapperParsingException {
            BooleanFieldMapper.Builder builder = new BooleanFieldMapper.Builder(name);
            parseField(builder, name, node);
            return builder;
        }
    }

    public static final class BooleanFieldType extends MappedFieldType {

        public BooleanFieldType(String name, boolean isSearchable, boolean hasDocValues) {
            super(name, isSearchable, hasDocValues);
        }

        public BooleanFieldType(String name) {
            this(name, true, true);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }
    }

    protected BooleanFieldMapper(String simpleName,
                                 int position,
                                 long columnOID,
                                 @Nullable String defaultExpression,
                                 FieldType fieldType,
                                 MappedFieldType defaultFieldType,
                                 CopyTo copyTo) {
        super(simpleName, position, columnOID, defaultExpression, fieldType, defaultFieldType, copyTo);
    }

    @Override
    public BooleanFieldType fieldType() {
        return (BooleanFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(ParseContext context, Consumer<IndexableField> onField) throws IOException {
        if (fieldType().isSearchable() == false && !fieldType.stored() && !fieldType().hasDocValues()) {
            return;
        }

        XContentParser.Token token = context.parser().currentToken();
        final Boolean value;
        if (token == XContentParser.Token.VALUE_NULL) {
            value = null;
        } else {
            value = context.parser().booleanValue();
        }

        if (value == null) {
            return;
        }
        if (fieldType().isSearchable() || fieldType.stored()) {
            onField.accept(new Field(fieldType().name(), value ? "T" : "F", fieldType));
        }
        if (fieldType().hasDocValues()) {
            onField.accept(new SortedNumericDocValuesField(fieldType().name(), value ? 1 : 0));
        } else {
            createFieldNamesField(context, onField);
        }
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {

    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
