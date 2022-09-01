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

import javax.annotation.Nullable;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

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
        public static final int IGNORE_ABOVE = Integer.MAX_VALUE;
    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        protected String nullValue = Defaults.NULL_VALUE;
        protected int ignoreAbove = Defaults.IGNORE_ABOVE;
        private Integer lengthLimit;
        private boolean blankPadding = false;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder ignoreAbove(int ignoreAbove) {
            if (ignoreAbove < 0) {
                throw new IllegalArgumentException("[ignore_above] must be positive, got " + ignoreAbove);
            }
            this.ignoreAbove = ignoreAbove;
            return this;
        }

        public Builder lengthLimit(int lengthLimit) {
            if (lengthLimit < 0) {
                throw new IllegalArgumentException("[legnth_limit] must be positive, got " + lengthLimit);
            }
            this.lengthLimit = lengthLimit;
            return this;
        }

        public Builder blankPadding(boolean blankPadding) {
            this.blankPadding = blankPadding;
            return this;
        }

        @Override
        public Builder indexOptions(IndexOptions indexOptions) {
            if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) > 0) {
                throw new IllegalArgumentException("The [keyword] field does not support positions, got [index_options]="
                        + indexOptionToString(indexOptions));
            }
            return super.indexOptions(indexOptions);
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return builder;
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
                defaultExpression,
                fieldType,
                buildFieldType(context),
                ignoreAbove,
                nullValue,
                lengthLimit,
                blankPadding,
                context.indexSettings(),
                copyTo
            );
            context.putPositionInfo(mapper, position);
            return mapper;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("ignore_above")) {
                    builder.ignoreAbove(XContentMapValues.nodeIntegerValue(propNode, -1));
                    iterator.remove();
                } else if (propName.equals("length_limit")) {
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

        boolean hasNorms;

        public KeywordFieldType(String name,
                                boolean isSearchable,
                                boolean hasDocValues,
                                boolean hasNorms) {
            super(name, isSearchable, hasDocValues);
            this.hasNorms = hasNorms;
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
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

    private int ignoreAbove;
    private final Integer lengthLimit;
    private final String nullValue;
    private final boolean blankPadding;


    private KeywordFieldMapper(String simpleName,
                               Integer position,
                               @Nullable String defaultExpression,
                               FieldType fieldType,
                               MappedFieldType mappedFieldType,
                               int ignoreAbove,
                               String nullValue,
                               Integer lengthLimit,
                               boolean blankPadding,
                               Settings indexSettings,
                               CopyTo copyTo) {
        super(simpleName, position, defaultExpression, fieldType, mappedFieldType, indexSettings, copyTo);
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
        this.ignoreAbove = ignoreAbove;
        this.lengthLimit = lengthLimit;
        this.blankPadding = blankPadding;
        this.nullValue = nullValue;
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
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        String value;
        XContentParser parser = context.parser();
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            value = nullValue;
        } else {
            value = parser.textOrNull();
        }

        if (value == null || value.length() > ignoreAbove) {
            return;
        }

        // convert to utf8 only once before feeding postings/dv/stored fields
        final BytesRef binaryValue = new BytesRef(value);
        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            Field field = new Field(fieldType().name(), binaryValue, fieldType);
            fields.add(field);

            if (fieldType().hasDocValues() == false && fieldType.omitNorms()) {
                createFieldNamesField(context, fields);
            }
        }

        if (fieldType().hasDocValues()) {
            fields.add(new SortedSetDocValuesField(fieldType().name(), binaryValue));
        }
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
        this.ignoreAbove = k.ignoreAbove;
        this.fieldType().setSearchAnalyzer(k.fieldType().searchAnalyzer());
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);


        if (includeDefaults || ignoreAbove != Defaults.IGNORE_ABOVE) {
            builder.field("ignore_above", ignoreAbove);
        }

        if (includeDefaults || lengthLimit != null) {
            builder.field("length_limit", lengthLimit);
        }

        if (includeDefaults || blankPadding) {
            builder.field("blank_padding", blankPadding);
        }
    }
}
