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

import static org.elasticsearch.index.mapper.TypeParsers.parseTextField;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.NormsFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;

/** A {@link FieldMapper} for full-text fields. */
public class TextFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "text";

    public static final String FAST_PHRASE_SUFFIX = "._index_phrase";

    public static class Defaults {
        public static final int FIELDDATA_MIN_SEGMENT_SIZE = 0;
        public static final int INDEX_PREFIX_MIN_CHARS = 2;
        public static final int INDEX_PREFIX_MAX_CHARS = 5;

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(true);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setStoreTermVectors(false);
            FIELD_TYPE.setOmitNorms(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            FIELD_TYPE.freeze();
        }

        /**
         * The default position_increment_gap is set to 100 so that phrase
         * queries of reasonably high slop will not match across field values.
         */
        public static final int POSITION_INCREMENT_GAP = 100;
    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public Builder docValues(boolean docValues) {
            if (docValues) {
                throw new IllegalArgumentException("[text] fields do not support doc values");
            }
            return super.docValues(docValues);
        }


        private TextFieldType buildFieldType(BuilderContext context) {
            TextFieldType ft = new TextFieldType(
                buildFullName(context),
                indexed,
                fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0);
            ft.setIndexAnalyzer(indexAnalyzer);
            ft.setSearchAnalyzer(searchAnalyzer);
            ft.setSearchQuoteAnalyzer(searchQuoteAnalyzer);
            return ft;
        }


        @Override
        public TextFieldMapper build(BuilderContext context) {
            TextFieldType tft = buildFieldType(context);
            return new TextFieldMapper(
                name,
                position,
                defaultExpression,
                fieldType,
                tft,
                context.indexSettings(),
                multiFieldsBuilder.build(this, context),
                copyTo
            );
        }


    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String fieldName, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            TextFieldMapper.Builder builder = new TextFieldMapper.Builder(fieldName);
            builder.indexAnalyzer(parserContext.getIndexAnalyzers().getDefaultIndexAnalyzer());
            builder.searchAnalyzer(parserContext.getIndexAnalyzers().getDefaultSearchAnalyzer());
            builder.searchQuoteAnalyzer(parserContext.getIndexAnalyzers().getDefaultSearchQuoteAnalyzer());
            parseTextField(builder, fieldName, node, parserContext);
            return builder;
        }
    }

    public static final class TextFieldType extends StringFieldType {

        public TextFieldType(String name, boolean indexed, boolean hasPositions) {
            super(name, indexed, false);
            this.hasPositions = hasPositions;
        }

        public TextFieldType(String name) {
            this(name, true, true);
        }

        protected TextFieldType(TextFieldType ref) {
            super(ref);
            this.hasPositions = ref.hasPositions;
        }

        public TextFieldType clone() {
            return new TextFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (context.getMapperService().getLuceneFieldType(name()).omitNorms()) {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            } else {
                return new NormsFieldExistsQuery(name());
            }
        }
    }

    protected TextFieldMapper(String simpleName,
                              Integer position,
                              String defaultExpression,
                              FieldType fieldType,
                              TextFieldType mappedFieldType,
                              Settings indexSettings,
                              MultiFields multiFields,
                              CopyTo copyTo) {
        super(simpleName, position, defaultExpression, fieldType, mappedFieldType, indexSettings, multiFields, copyTo);
        assert mappedFieldType.hasDocValues() == false;
    }

    @Override
    protected TextFieldMapper clone() {
        return (TextFieldMapper) super.clone();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        final String value;
        if (context.externalValueSet()) {
            value = context.externalValue().toString();
        } else {
            value = context.parser().textOrNull();
        }

        if (value == null) {
            return;
        }

        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            Field field = new Field(fieldType().name(), value, fieldType);
            fields.add(field);
            if (fieldType.omitNorms()) {
                createFieldNamesField(context, fields);
            }
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
    }

    @Override
    public TextFieldType fieldType() {
        return (TextFieldType) super.fieldType();
    }

    @Override
    protected boolean docValuesByDefault() {
        return false;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        doXContentAnalyzers(builder, includeDefaults);
    }
}
