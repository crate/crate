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

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.TypeParsers.parseField;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;

/**
 * A field mapper for boolean fields.
 */
public class BooleanFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "boolean";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new BooleanFieldType();

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.freeze();
        }
    }

    public static class Values {
        public static final BytesRef TRUE = new BytesRef("T");
        public static final BytesRef FALSE = new BytesRef("F");
    }

    public static class Builder extends FieldMapper.Builder<Builder, BooleanFieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            this.builder = this;
        }

        @Override
        public BooleanFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new BooleanFieldMapper(
                name,
                position,
                defaultExpression,
                fieldType,
                defaultFieldType,
                context.indexSettings(),
                multiFieldsBuilder.build(this, context),
                copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            BooleanFieldMapper.Builder builder = new BooleanFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(nodeBooleanValue(propNode, name + ".null_value"));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class BooleanFieldType extends TermBasedFieldType {

        public BooleanFieldType() {
        }

        protected BooleanFieldType(BooleanFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new BooleanFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }

        @Override
        public Boolean nullValue() {
            return (Boolean)super.nullValue();
        }

        @Override
        public BytesRef indexedValueForSearch(Object value) {
            if (value == null) {
                return Values.FALSE;
            }
            if (value instanceof Boolean) {
                return ((Boolean) value) ? Values.TRUE : Values.FALSE;
            }
            String sValue;
            if (value instanceof BytesRef) {
                sValue = ((BytesRef) value).utf8ToString();
            } else {
                sValue = value.toString();
            }
            switch (sValue) {
                case "true":
                    return Values.TRUE;
                case "false":
                    return Values.FALSE;
                default:
                    throw new IllegalArgumentException("Can't parse boolean value [" +
                                    sValue + "], expected [true] or [false]");
            }
        }

        @Override
        public Boolean valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            switch (value.toString()) {
                case "F":
                    return false;
                case "T":
                    return true;
                default:
                    throw new IllegalArgumentException("Expected [T] or [F] but got [" + value + "]");
            }
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
            failIfNotIndexed();
            return new TermRangeQuery(name(),
                lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
                upperTerm == null ? null : indexedValueForSearch(upperTerm),
                includeLower, includeUpper);
        }
    }

    protected BooleanFieldMapper(String simpleName,
                                 Integer position,
                                 @Nullable String defaultExpression,
                                 MappedFieldType fieldType,
                                 MappedFieldType defaultFieldType,
                                 Settings indexSettings,
                                 MultiFields multiFields,
                                 CopyTo copyTo) {
        super(simpleName, position, defaultExpression, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
    }

    @Override
    public BooleanFieldType fieldType() {
        return (BooleanFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        if (fieldType().indexOptions() == IndexOptions.NONE && !fieldType().stored() && !fieldType().hasDocValues()) {
            return;
        }

        Boolean value = context.parseExternalValue(Boolean.class);
        if (value == null) {
            XContentParser.Token token = context.parser().currentToken();
            if (token == XContentParser.Token.VALUE_NULL) {
                if (fieldType().nullValue() != null) {
                    value = fieldType().nullValue();
                }
            } else {
                value = context.parser().booleanValue();
            }
        }

        if (value == null) {
            return;
        }
        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            fields.add(new Field(fieldType().name(), value ? "T" : "F", fieldType()));
        }
        if (fieldType().hasDocValues()) {
            fields.add(new SortedNumericDocValuesField(fieldType().name(), value ? 1 : 0));
        } else {
            createFieldNamesField(context, fields);
        }
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {

    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || fieldType().nullValue() != null) {
            builder.field("null_value", fieldType().nullValue());
        }
    }
}
