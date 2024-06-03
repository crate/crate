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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.jetbrains.annotations.Nullable;

import io.crate.server.xcontent.XContentMapValues;

/**
 * A field mapper for keywords. This mapper accepts strings and indexes them as-is.
 */
public final class KeywordFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "keyword";

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }

        public static final String NULL_VALUE = null;
    }

    public static class Builder extends FieldMapper.Builder {

        protected String nullValue = Defaults.NULL_VALUE;
        private Integer lengthLimit;
        private boolean blankPadding = false;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
        }

        public void lengthLimit(int lengthLimit) {
            if (lengthLimit < 0) {
                throw new IllegalArgumentException("[legnth_limit] must be positive, got " + lengthLimit);
            }
            this.lengthLimit = lengthLimit;
        }

        public void blankPadding(boolean blankPadding) {
            this.blankPadding = blankPadding;
        }

        @Override
        public void indexOptions(IndexOptions indexOptions) {
            if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) > 0) {
                throw new IllegalArgumentException("The [keyword] field does not support positions, got [index_options]="
                        + indexOptionToString(indexOptions));
            }
        }

        public void nullValue(String nullValue) {
            this.nullValue = nullValue;
        }

        private KeywordFieldType buildFieldType(BuilderContext context) {
            return new KeywordFieldType(
                buildFullName(context),
                indexed,
                hasDocValues,
                fieldType.omitNorms() == false
            );
        }

        @Override
        public KeywordFieldMapper build(BuilderContext context) {
            var mapper = new KeywordFieldMapper(
                name,
                position,
                columnOID,
                isDropped,
                defaultExpression,
                fieldType,
                buildFieldType(context),
                nullValue,
                lengthLimit,
                blankPadding,
                copyTo
            );
            context.putPositionInfo(mapper, position);
            return mapper;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder(name);
            parseField(builder, name, node);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("length_limit")) {
                    builder.lengthLimit(XContentMapValues.nodeIntegerValue(propNode, -1));
                    iterator.remove();
                } else if (propName.equals("blank_padding")) {
                    builder.blankPadding(XContentMapValues.nodeBooleanValue(propNode));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class KeywordFieldType extends MappedFieldType {

        public KeywordFieldType(String name,
                                boolean isSearchable,
                                boolean hasDocValues,
                                boolean hasNorms) {
            super(name, isSearchable, hasDocValues, hasNorms);
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
        }

        public KeywordFieldType(String name, boolean isSearchable, boolean hasDocValues) {
            this(name, isSearchable, hasDocValues, true);
        }

        public KeywordFieldType(String name) {
            this(name, true, true);
        }


        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }
    }

    private final Integer lengthLimit;
    private final boolean blankPadding;


    private KeywordFieldMapper(String simpleName,
                               int position,
                               long columnOID,
                               boolean isDropped,
                               @Nullable String defaultExpression,
                               FieldType fieldType,
                               MappedFieldType mappedFieldType,
                               String nullValue,
                               Integer lengthLimit,
                               boolean blankPadding,
                               CopyTo copyTo) {
        super(simpleName, position, columnOID, isDropped, defaultExpression, fieldType, mappedFieldType, copyTo);
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
        this.lengthLimit = lengthLimit;
        this.blankPadding = blankPadding;
    }

    @Override
    protected KeywordFieldMapper clone() {
        return (KeywordFieldMapper) super.clone();
    }

    @Override
    public KeywordFieldType fieldType() {
        return (KeywordFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        KeywordFieldMapper k = (KeywordFieldMapper) other;
        if (!Objects.equals(this.lengthLimit, k.lengthLimit)) {
            throw new IllegalArgumentException(
                "mapper [" + name() + "] has different length_limit settings, current ["
                + this.lengthLimit + "], merged [" + k.lengthLimit + "]");
        }
        if (this.blankPadding != k.blankPadding) {
            throw new IllegalArgumentException(
                "mapper [" + name() + "] has different blank_padding settings, current ["
                + this.blankPadding + "], merged [" + k.blankPadding + "]");
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults) throws IOException {
        super.doXContentBody(builder, includeDefaults);

        if (includeDefaults || lengthLimit != null) {
            builder.field("length_limit", lengthLimit);
        }

        if (includeDefaults || blankPadding) {
            builder.field("blank_padding", blankPadding);
        }
    }
}
