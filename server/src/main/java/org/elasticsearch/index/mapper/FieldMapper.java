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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper.FieldNamesFieldType;

public abstract class FieldMapper extends Mapper implements Cloneable {

    public abstract static class Builder<T extends Builder<T>> extends Mapper.Builder<T> {

        protected final FieldType fieldType;
        protected boolean indexOptionsSet = false;
        protected boolean hasDocValues = true;
        protected boolean indexed = true;
        protected CopyTo copyTo = CopyTo.empty();
        @Nullable
        protected String defaultExpression;

        protected Builder(String name, FieldType fieldType) {
            super(name);
            this.fieldType = new FieldType(fieldType);
        }

        public T index(boolean index) {
            this.indexed = index;
            if (index == false) {
                this.fieldType.setIndexOptions(IndexOptions.NONE);
            }
            return builder;
        }

        public T store(boolean store) {
            this.fieldType.setStored(store);
            return builder;
        }

        public T docValues(boolean docValues) {
            this.hasDocValues = docValues;
            return builder;
        }

        public T indexOptions(IndexOptions indexOptions) {
            this.fieldType.setIndexOptions(indexOptions);
            this.indexOptionsSet = true;
            return builder;
        }

        public T copyTo(CopyTo copyTo) {
            this.copyTo = copyTo;
            return builder;
        }

        protected String buildFullName(BuilderContext context) {
            return context.path().pathAsText(name);
        }

        public void defaultExpression(String defaultExpression) {
            this.defaultExpression = defaultExpression;
        }
    }

    protected final Version indexCreatedVersion;
    protected FieldType fieldType;
    protected MappedFieldType mappedFieldType;
    protected CopyTo copyTo;

    /**
     * Expression that is used as the default value for a field
     */
    @Nullable String defaultExpression;

    protected FieldMapper(String simpleName,
                          int position,
                          @Nullable String defaultExpression,
                          FieldType fieldType,
                          MappedFieldType mappedFieldType,
                          Settings indexSettings,
                          CopyTo copyTo) {
        super(simpleName);
        assert indexSettings != null;
        this.position = position;
        this.defaultExpression = defaultExpression;
        this.indexCreatedVersion = Version.indexCreated(indexSettings);
        if (mappedFieldType.name().isEmpty()) {
            throw new IllegalArgumentException("name cannot be empty string");
        }
        fieldType.freeze();
        this.fieldType = fieldType;
        this.mappedFieldType = mappedFieldType;
        this.copyTo = Objects.requireNonNull(copyTo);
    }

    public int position() {
        return position;
    }

    @Override
    public int maxColumnPosition() {
        return position;
    }

    @Override
    public String name() {
        return fieldType().name();
    }

    @Override
    public String typeName() {
        return mappedFieldType.typeName();
    }

    public MappedFieldType fieldType() {
        return mappedFieldType;
    }

    /**
     * List of fields where this field should be copied to
     */
    public CopyTo copyTo() {
        return copyTo;
    }

    /**
     * Parse the field value using the provided {@link ParseContext}.
     */
    public void parse(ParseContext context) throws IOException {
        final List<IndexableField> fields = new ArrayList<>(2);
        try {
            parseCreateField(context, fields);
            for (IndexableField field : fields) {
                context.doc().add(field);
            }
        } catch (Exception e) {
            throw new MapperParsingException("failed to parse field [{}] of type [{}]", e, fieldType().name(),
                    fieldType().typeName());
        }
    }

    /**
     * Parse the field value and populate <code>fields</code>.
     */
    protected abstract void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException;

    protected void createFieldNamesField(ParseContext context, List<IndexableField> fields) {
        FieldNamesFieldType fieldNamesFieldType = context.docMapper()
            .metadataMapper(FieldNamesFieldMapper.class)
            .fieldType();
        if (fieldNamesFieldType != null && fieldNamesFieldType.isEnabled()) {
            for (String fieldName : FieldNamesFieldMapper.extractFieldNames(fieldType().name())) {
                fields.add(new Field(FieldNamesFieldMapper.NAME, fieldName, FieldNamesFieldMapper.Defaults.FIELD_TYPE));
            }
        }
    }

    @Override
    public Iterator<Mapper> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    protected FieldMapper clone() {
        try {
            return (FieldMapper) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public FieldMapper merge(Mapper mergeWith) {
        FieldMapper merged = clone();
        List<String> conflicts = new ArrayList<>();
        if (mergeWith instanceof FieldMapper == false) {
            throw new IllegalArgumentException("mapper [" + mappedFieldType.name() + "] cannot be changed from type ["
            + contentType() + "] to [" + mergeWith.getClass().getSimpleName() + "]");
        }
        FieldMapper toMerge = (FieldMapper) mergeWith;
        merged.mergeSharedOptions(toMerge, conflicts);
        merged.mergeOptions(toMerge, conflicts);
        if (conflicts.isEmpty() == false) {
            throw new IllegalArgumentException("Mapper for [" + name() +
                "] conflicts with existing mapping:\n" + conflicts.toString());
        }
        // apply changeable values
        merged.mappedFieldType = toMerge.mappedFieldType;
        merged.fieldType = toMerge.fieldType;
        merged.copyTo = toMerge.copyTo;
        return merged;
    }

    private void mergeSharedOptions(FieldMapper mergeWith, List<String> conflicts) {

        if (Objects.equals(this.contentType(), mergeWith.contentType()) == false) {
            throw new IllegalArgumentException("mapper [" + fieldType().name() + "] cannot be changed from type [" + contentType()
                + "] to [" + mergeWith.contentType() + "]");
        }

        FieldType other = mergeWith.fieldType;
        final MappedFieldType otherm = mergeWith.mappedFieldType;

        boolean indexed = fieldType.indexOptions() != IndexOptions.NONE;
        boolean mergeWithIndexed = other.indexOptions() != IndexOptions.NONE;
        if (indexed != mergeWithIndexed) {
            conflicts.add("mapper [" + name() + "] has different [index] values");
        }
        // TODO: should be validating if index options go "up" (but "down" is ok)
        if (fieldType.indexOptions() != other.indexOptions()) {
            conflicts.add("mapper [" + name() + "] has different [index_options] values");
        }
        if (fieldType.stored() != other.stored()) {
            conflicts.add("mapper [" + name() + "] has different [store] values");
        }
        if (this.mappedFieldType.hasDocValues() != otherm.hasDocValues()) {
            conflicts.add("mapper [" + name() + "] has different [doc_values] values");
        }
        if (fieldType.omitNorms() && !other.omitNorms()) {
            conflicts.add("mapper [" + name() + "] has different [norms] values, cannot change from disable to enabled");
        }
        if (fieldType.storeTermVectors() != other.storeTermVectors()) {
            conflicts.add("mapper [" + name() + "] has different [store_term_vector] values");
        }
        if (fieldType.storeTermVectorOffsets() != other.storeTermVectorOffsets()) {
            conflicts.add("mapper [" + name() + "] has different [store_term_vector_offsets] values");
        }
        if (fieldType.storeTermVectorPositions() != other.storeTermVectorPositions()) {
            conflicts.add("mapper [" + name() + "] has different [store_term_vector_positions] values");
        }
        if (fieldType.storeTermVectorPayloads() != other.storeTermVectorPayloads()) {
            conflicts.add("mapper [" + name() + "] has different [store_term_vector_payloads] values");
        }

        // null and "default"-named index analyzers both mean the default is used
        if (mappedFieldType.indexAnalyzer() == null || "default".equals(mappedFieldType.indexAnalyzer().name())) {
            if (otherm.indexAnalyzer() != null && "default".equals(otherm.indexAnalyzer().name()) == false) {
                conflicts.add("mapper [" + name() + "] has different [analyzer]");
            }
        } else if (otherm.indexAnalyzer() == null || "default".equals(otherm.indexAnalyzer().name())) {
            conflicts.add("mapper [" + name() + "] has different [analyzer]");
        } else if (mappedFieldType.indexAnalyzer().name().equals(otherm.indexAnalyzer().name()) == false) {
            conflicts.add("mapper [" + name() + "] has different [analyzer]");
        }
    }

    /**
     * Merge type-specific options and check for incompatible settings in mappings to be merged
     */
    protected abstract void mergeOptions(FieldMapper other, List<String> conflicts);

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName());
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
        doXContentBody(builder, includeDefaults, params);
        return builder.endObject();
    }

    protected boolean indexedByDefault() {
        return true;
    }

    protected boolean docValuesByDefault() {
        return true;
    }

    protected boolean storedByDefault() {
        return false;
    }

    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {

        builder.field("type", contentType());

        if (includeDefaults || mappedFieldType.isSearchable() != indexedByDefault()) {
            builder.field("index", mappedFieldType.isSearchable());
        }
        if (includeDefaults || mappedFieldType.hasDocValues() != docValuesByDefault()) {
            builder.field("doc_values", mappedFieldType.hasDocValues());
        }
        if (position != NOT_TO_BE_POSITIONED) {
            builder.field("position", position);
        }
        if (defaultExpression != null) {
            builder.field("default_expr", defaultExpression);
        }
        if (includeDefaults || fieldType.stored() != storedByDefault()) {
            builder.field("store", fieldType.stored());
        }

        copyTo.toXContent(builder, params);
    }

    protected final void doXContentAnalyzers(XContentBuilder builder, boolean includeDefaults) throws IOException {
        if (fieldType.tokenized() == false) {
            return;
        }
        if (fieldType().indexAnalyzer() == null) {
            if (includeDefaults) {
                builder.field("analyzer", "default");
            }
        } else {
            boolean hasDefaultIndexAnalyzer = fieldType().indexAnalyzer().name().equals("default");
            final String searchAnalyzerName = fieldType().searchAnalyzer().name();
            boolean hasDifferentSearchAnalyzer = searchAnalyzerName.equals(fieldType().indexAnalyzer().name()) == false;
            boolean hasDifferentSearchQuoteAnalyzer = searchAnalyzerName.equals(fieldType().searchQuoteAnalyzer().name()) == false;
            if (includeDefaults || hasDefaultIndexAnalyzer == false || hasDifferentSearchAnalyzer || hasDifferentSearchQuoteAnalyzer) {
                builder.field("analyzer", fieldType().indexAnalyzer().name());
                if (includeDefaults || hasDifferentSearchAnalyzer || hasDifferentSearchQuoteAnalyzer) {
                    builder.field("search_analyzer", searchAnalyzerName);
                    if (includeDefaults || hasDifferentSearchQuoteAnalyzer) {
                        builder.field("search_quote_analyzer", fieldType().searchQuoteAnalyzer().name());
                    }
                }
            }
        }
    }

    protected static String indexOptionToString(IndexOptions indexOption) {
        switch (indexOption) {
            case DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS:
                return TypeParsers.INDEX_OPTIONS_OFFSETS;
            case DOCS_AND_FREQS:
                return TypeParsers.INDEX_OPTIONS_FREQS;
            case DOCS_AND_FREQS_AND_POSITIONS:
                return TypeParsers.INDEX_OPTIONS_POSITIONS;
            case DOCS:
                return TypeParsers.INDEX_OPTIONS_DOCS;
            default:
                throw new IllegalArgumentException("Unknown IndexOptions [" + indexOption + "]");
        }
    }

    public static String termVectorOptionsToString(FieldType fieldType) {
        if (!fieldType.storeTermVectors()) {
            return "no";
        } else if (!fieldType.storeTermVectorOffsets() && !fieldType.storeTermVectorPositions()) {
            return "yes";
        } else if (fieldType.storeTermVectorOffsets() && !fieldType.storeTermVectorPositions()) {
            return "with_offsets";
        } else {
            StringBuilder builder = new StringBuilder("with");
            if (fieldType.storeTermVectorPositions()) {
                builder.append("_positions");
            }
            if (fieldType.storeTermVectorOffsets()) {
                builder.append("_offsets");
            }
            if (fieldType.storeTermVectorPayloads()) {
                builder.append("_payloads");
            }
            return builder.toString();
        }
    }

    protected abstract String contentType();

    /**
     * Represents a list of fields with optional boost factor where the current field should be copied to
     */
    public static class CopyTo {

        private static final CopyTo EMPTY = new CopyTo(Collections.emptyList());

        public static CopyTo empty() {
            return EMPTY;
        }

        private final List<String> copyToFields;

        private CopyTo(List<String> copyToFields) {
            this.copyToFields = copyToFields;
        }

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (!copyToFields.isEmpty()) {
                builder.startArray("copy_to");
                for (String field : copyToFields) {
                    builder.value(field);
                }
                builder.endArray();
            }
            return builder;
        }

        public static class Builder {
            private final List<String> copyToBuilders = new ArrayList<>();

            public Builder add(String field) {
                copyToBuilders.add(field);
                return this;
            }

            public CopyTo build() {
                if (copyToBuilders.isEmpty()) {
                    return EMPTY;
                }
                return new CopyTo(Collections.unmodifiableList(copyToBuilders));
            }
        }

        public List<String> copyToFields() {
            return copyToFields;
        }
    }

}
