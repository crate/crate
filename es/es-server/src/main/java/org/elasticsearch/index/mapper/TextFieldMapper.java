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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.shingle.FixedShingleFilter;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.NormsFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.Version;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.mapper.TypeParsers.parseTextField;

/** A {@link FieldMapper} for full-text fields. */
public class TextFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "text";
    private static final int POSITION_INCREMENT_GAP_USE_ANALYZER = -1;

    public static final String FAST_PHRASE_SUFFIX = "._index_phrase";

    public static class Defaults {
        public static final double FIELDDATA_MIN_FREQUENCY = 0;
        public static final double FIELDDATA_MAX_FREQUENCY = Integer.MAX_VALUE;
        public static final int FIELDDATA_MIN_SEGMENT_SIZE = 0;
        public static final int INDEX_PREFIX_MIN_CHARS = 2;
        public static final int INDEX_PREFIX_MAX_CHARS = 5;

        public static final MappedFieldType FIELD_TYPE = new TextFieldType();

        static {
            FIELD_TYPE.freeze();
        }

        /**
         * The default position_increment_gap is set to 100 so that phrase
         * queries of reasonably high slop will not match across field values.
         */
        public static final int POSITION_INCREMENT_GAP = 100;
    }

    public static class Builder extends FieldMapper.Builder<Builder, TextFieldMapper> {

        private int positionIncrementGap = POSITION_INCREMENT_GAP_USE_ANALYZER;
        private PrefixFieldType prefixFieldType;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public TextFieldType fieldType() {
            return (TextFieldType) super.fieldType();
        }

        public Builder positionIncrementGap(int positionIncrementGap) {
            if (positionIncrementGap < 0) {
                throw new MapperParsingException("[positions_increment_gap] must be positive, got " + positionIncrementGap);
            }
            this.positionIncrementGap = positionIncrementGap;
            return this;
        }

        public Builder fielddata(boolean fielddata) {
            fieldType().setFielddata(fielddata);
            return builder;
        }

        public Builder indexPhrases(boolean indexPhrases) {
            fieldType().setIndexPhrases(indexPhrases);
            return builder;
        }

        @Override
        public Builder docValues(boolean docValues) {
            if (docValues) {
                throw new IllegalArgumentException("[text] fields do not support doc values");
            }
            return super.docValues(docValues);
        }

        public Builder eagerGlobalOrdinals(boolean eagerGlobalOrdinals) {
            fieldType().setEagerGlobalOrdinals(eagerGlobalOrdinals);
            return builder;
        }

        public Builder fielddataFrequencyFilter(double minFreq, double maxFreq, int minSegmentSize) {
            fieldType().setFielddataMinFrequency(minFreq);
            fieldType().setFielddataMaxFrequency(maxFreq);
            fieldType().setFielddataMinSegmentSize(minSegmentSize);
            return builder;
        }

        public Builder indexPrefixes(int minChars, int maxChars) {
            if (minChars > maxChars) {
                throw new IllegalArgumentException("min_chars [" + minChars + "] must be less than max_chars [" + maxChars + "]");
            }
            if (minChars < 1) {
                throw new IllegalArgumentException("min_chars [" + minChars + "] must be greater than zero");
            }
            if (maxChars >= 20) {
                throw new IllegalArgumentException("max_chars [" + maxChars + "] must be less than 20");
            }
            this.prefixFieldType = new PrefixFieldType(name() + "._index_prefix", minChars, maxChars);
            fieldType().setPrefixFieldType(this.prefixFieldType);
            return this;
        }

        @Override
        public TextFieldMapper build(BuilderContext context) {
            if (positionIncrementGap != POSITION_INCREMENT_GAP_USE_ANALYZER) {
                if (fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                    throw new IllegalArgumentException("Cannot set position_increment_gap on field ["
                        + name + "] without positions enabled");
                }
                fieldType.setIndexAnalyzer(new NamedAnalyzer(fieldType.indexAnalyzer(), positionIncrementGap));
                fieldType.setSearchAnalyzer(new NamedAnalyzer(fieldType.searchAnalyzer(), positionIncrementGap));
                fieldType.setSearchQuoteAnalyzer(new NamedAnalyzer(fieldType.searchQuoteAnalyzer(), positionIncrementGap));
            }
            setupFieldType(context);
            PrefixFieldMapper prefixMapper = null;
            if (prefixFieldType != null) {
                if (fieldType().isSearchable() == false) {
                    throw new IllegalArgumentException("Cannot set index_prefixes on unindexed field [" + name() + "]");
                }
                // Copy the index options of the main field to allow phrase queries on
                // the prefix field.
                if (context.indexCreatedVersion().onOrAfter(Version.ES_V_6_5_1)) {
                    if (fieldType.indexOptions() == IndexOptions.DOCS_AND_FREQS) {
                        // frequencies are not needed because prefix queries always use a constant score
                        prefixFieldType.setIndexOptions(IndexOptions.DOCS);
                    } else {
                        prefixFieldType.setIndexOptions(fieldType.indexOptions());
                    }
                } else if (fieldType.indexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) {
                    prefixFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
                }
                if (fieldType.storeTermVectorOffsets()) {
                    prefixFieldType.setStoreTermVectorOffsets(true);
                }
                prefixFieldType.setAnalyzer(fieldType.indexAnalyzer());
                prefixMapper = new PrefixFieldMapper(prefixFieldType, context.indexSettings());
            }
            if (fieldType().indexPhrases) {
                if (fieldType().isSearchable() == false) {
                    throw new IllegalArgumentException("Cannot set index_phrases on unindexed field [" + name() + "]");
                }
                if (fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                    throw new IllegalArgumentException("Cannot set index_phrases on field [" + name() + "] if positions are not enabled");
                }
            }
            return new TextFieldMapper(
                name,
                position,
                fieldType(),
                defaultFieldType,
                positionIncrementGap,
                prefixMapper,
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
            builder.fieldType().setIndexAnalyzer(parserContext.getIndexAnalyzers().getDefaultIndexAnalyzer());
            builder.fieldType().setSearchAnalyzer(parserContext.getIndexAnalyzers().getDefaultSearchAnalyzer());
            builder.fieldType().setSearchQuoteAnalyzer(parserContext.getIndexAnalyzers().getDefaultSearchQuoteAnalyzer());
            parseTextField(builder, fieldName, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("position_increment_gap")) {
                    int newPositionIncrementGap = XContentMapValues.nodeIntegerValue(propNode, -1);
                    builder.positionIncrementGap(newPositionIncrementGap);
                    iterator.remove();
                } else if (propName.equals("fielddata")) {
                    builder.fielddata(XContentMapValues.nodeBooleanValue(propNode, "fielddata"));
                    iterator.remove();
                } else if (propName.equals("eager_global_ordinals")) {
                    builder.eagerGlobalOrdinals(XContentMapValues.nodeBooleanValue(propNode, "eager_global_ordinals"));
                    iterator.remove();
                } else if (propName.equals("fielddata_frequency_filter")) {
                    Map<?,?> frequencyFilter = (Map<?, ?>) propNode;
                    double minFrequency = XContentMapValues.nodeDoubleValue(frequencyFilter.remove("min"), 0);
                    double maxFrequency = XContentMapValues.nodeDoubleValue(frequencyFilter.remove("max"), Integer.MAX_VALUE);
                    int minSegmentSize = XContentMapValues.nodeIntegerValue(frequencyFilter.remove("min_segment_size"), 0);
                    builder.fielddataFrequencyFilter(minFrequency, maxFrequency, minSegmentSize);
                    DocumentMapperParser.checkNoRemainingFields(propName, frequencyFilter, parserContext.indexVersionCreated());
                    iterator.remove();
                } else if (propName.equals("index_prefixes")) {
                    Map<?, ?> indexPrefix = (Map<?, ?>) propNode;
                    int minChars = XContentMapValues.nodeIntegerValue(indexPrefix.remove("min_chars"),
                        Defaults.INDEX_PREFIX_MIN_CHARS);
                    int maxChars = XContentMapValues.nodeIntegerValue(indexPrefix.remove("max_chars"),
                        Defaults.INDEX_PREFIX_MAX_CHARS);
                    builder.indexPrefixes(minChars, maxChars);
                    DocumentMapperParser.checkNoRemainingFields(propName, indexPrefix, parserContext.indexVersionCreated());
                    iterator.remove();
                } else if (propName.equals("index_phrases")) {
                    builder.indexPhrases(XContentMapValues.nodeBooleanValue(propNode, "index_phrases"));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    private static class PhraseWrappedAnalyzer extends AnalyzerWrapper {

        private final Analyzer delegate;

        PhraseWrappedAnalyzer(Analyzer delegate) {
            super(delegate.getReuseStrategy());
            this.delegate = delegate;
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            return delegate;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
            return new TokenStreamComponents(components.getSource(), new FixedShingleFilter(components.getTokenStream(), 2));
        }
    }

    private static class PrefixWrappedAnalyzer extends AnalyzerWrapper {

        private final int minChars;
        private final int maxChars;
        private final Analyzer delegate;

        PrefixWrappedAnalyzer(Analyzer delegate, int minChars, int maxChars) {
            super(delegate.getReuseStrategy());
            this.delegate = delegate;
            this.minChars = minChars;
            this.maxChars = maxChars;
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            return delegate;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
            TokenFilter filter = new EdgeNGramTokenFilter(components.getTokenStream(), minChars, maxChars, false);
            return new TokenStreamComponents(components.getSource(), filter);
        }
    }

    private static final class PhraseFieldType extends StringFieldType {

        final TextFieldType parent;

        PhraseFieldType(TextFieldType parent) {
            setTokenized(true);
            setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            if (parent.indexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) {
                setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
            }
            if (parent.storeTermVectorOffsets()) {
                setStoreTermVectors(true);
                setStoreTermVectorPositions(true);
                setStoreTermVectorOffsets(true);
            }
            setAnalyzer(parent.indexAnalyzer().name(), parent.indexAnalyzer().analyzer());
            setName(parent.name() + FAST_PHRASE_SUFFIX);
            this.parent = parent;
        }

        void setAnalyzer(String name, Analyzer delegate) {
            setIndexAnalyzer(new NamedAnalyzer(name, AnalyzerScope.INDEX, new PhraseWrappedAnalyzer(delegate)));
        }

        @Override
        public MappedFieldType clone() {
            return new PhraseFieldType(parent);
        }

        @Override
        public String typeName() {
            return "phrase";
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new UnsupportedOperationException();
        }
    }

    static final class PrefixFieldType extends StringFieldType {

        final int minChars;
        final int maxChars;

        PrefixFieldType(String name, int minChars, int maxChars) {
            setTokenized(true);
            setOmitNorms(true);
            setIndexOptions(IndexOptions.DOCS);
            setName(name);
            this.minChars = minChars;
            this.maxChars = maxChars;
        }

        PrefixFieldType setAnalyzer(NamedAnalyzer delegate) {
            setIndexAnalyzer(new NamedAnalyzer(delegate.name(), AnalyzerScope.INDEX,
                new PrefixWrappedAnalyzer(delegate.analyzer(), minChars, maxChars)));
            return this;
        }

        boolean accept(int length) {
            return length >= minChars && length <= maxChars;
        }

        void doXContent(XContentBuilder builder) throws IOException {
            builder.startObject("index_prefixes");
            builder.field("min_chars", minChars);
            builder.field("max_chars", maxChars);
            builder.endObject();
        }

        @Override
        public PrefixFieldType clone() {
            return new PrefixFieldType(name(), minChars, maxChars);
        }

        @Override
        public String typeName() {
            return "prefix";
        }

        @Override
        public String toString() {
            return super.toString() + ",prefixChars=" + minChars + ":" + maxChars;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            PrefixFieldType that = (PrefixFieldType) o;
            return minChars == that.minChars &&
                maxChars == that.maxChars;
        }

        @Override
        public int hashCode() {

            return Objects.hash(super.hashCode(), minChars, maxChars);
        }
    }

    private static final class PhraseFieldMapper extends FieldMapper {

        PhraseFieldMapper(PhraseFieldType fieldType, Settings indexSettings) {
            super(fieldType.name(), null, fieldType, fieldType, indexSettings, MultiFields.empty(), CopyTo.empty());
        }

        @Override
        protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String contentType() {
            return "phrase";
        }
    }

    private static final class PrefixFieldMapper extends FieldMapper {

        protected PrefixFieldMapper(PrefixFieldType fieldType, Settings indexSettings) {
            super(fieldType.name(), null, fieldType, fieldType, indexSettings, MultiFields.empty(), CopyTo.empty());
        }

        void addField(String value, List<IndexableField> fields) {
            fields.add(new Field(fieldType().name(), value, fieldType()));
        }

        @Override
        protected void parseCreateField(ParseContext context, List<IndexableField> fields) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String contentType() {
            return "prefix";
        }

        @Override
        public String toString() {
            return fieldType().toString();
        }
    }

    public static final class TextFieldType extends StringFieldType {

        private boolean fielddata;
        private double fielddataMinFrequency;
        private double fielddataMaxFrequency;
        private int fielddataMinSegmentSize;
        private PrefixFieldType prefixFieldType;
        private boolean indexPhrases = false;

        public TextFieldType() {
            setTokenized(true);
            fielddata = false;
            fielddataMinFrequency = Defaults.FIELDDATA_MIN_FREQUENCY;
            fielddataMaxFrequency = Defaults.FIELDDATA_MAX_FREQUENCY;
            fielddataMinSegmentSize = Defaults.FIELDDATA_MIN_SEGMENT_SIZE;
        }

        protected TextFieldType(TextFieldType ref) {
            super(ref);
            this.fielddata = ref.fielddata;
            this.fielddataMinFrequency = ref.fielddataMinFrequency;
            this.fielddataMaxFrequency = ref.fielddataMaxFrequency;
            this.fielddataMinSegmentSize = ref.fielddataMinSegmentSize;
            this.indexPhrases = ref.indexPhrases;
            if (ref.prefixFieldType != null) {
                this.prefixFieldType = ref.prefixFieldType.clone();
            }
        }

        public TextFieldType clone() {
            return new TextFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o) == false) {
                return false;
            }
            TextFieldType that = (TextFieldType) o;
            return fielddata == that.fielddata
                    && indexPhrases == that.indexPhrases
                    && Objects.equals(prefixFieldType, that.prefixFieldType)
                    && fielddataMinFrequency == that.fielddataMinFrequency
                    && fielddataMaxFrequency == that.fielddataMaxFrequency
                    && fielddataMinSegmentSize == that.fielddataMinSegmentSize;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), fielddata, indexPhrases, prefixFieldType,
                    fielddataMinFrequency, fielddataMaxFrequency, fielddataMinSegmentSize);
        }

        @Override
        public void checkCompatibility(MappedFieldType other,
                List<String> conflicts, boolean strict) {
            super.checkCompatibility(other, conflicts, strict);
            TextFieldType otherType = (TextFieldType) other;
            if (Objects.equals(this.prefixFieldType, otherType.prefixFieldType) == false) {
                if (this.prefixFieldType == null) {
                    conflicts.add("mapper [" + name()
                        + "] has different [index_prefixes] settings, cannot change from disabled to enabled");
                }
                else if (otherType.prefixFieldType == null) {
                    conflicts.add("mapper [" + name()
                        + "] has different [index_prefixes] settings, cannot change from enabled to disabled");
                }
                else {
                    conflicts.add("mapper [" + name() + "] has different [index_prefixes] settings");
                }
            }
            if (this.indexPhrases != otherType.indexPhrases) {
                conflicts.add("mapper [" + name() + "] has different [index_phrases] settings");
            }
            if (strict) {
                if (fielddata() != otherType.fielddata()) {
                    conflicts.add("mapper [" + name() + "] is used by multiple types. Set update_all_types to true to update [fielddata] "
                            + "across all types.");
                }
                if (fielddataMinFrequency() != otherType.fielddataMinFrequency()) {
                    conflicts.add("mapper [" + name() + "] is used by multiple types. Set update_all_types to true to update "
                            + "[fielddata_frequency_filter.min] across all types.");
                }
                if (fielddataMaxFrequency() != otherType.fielddataMaxFrequency()) {
                    conflicts.add("mapper [" + name() + "] is used by multiple types. Set update_all_types to true to update "
                            + "[fielddata_frequency_filter.max] across all types.");
                }
                if (fielddataMinSegmentSize() != otherType.fielddataMinSegmentSize()) {
                    conflicts.add("mapper [" + name() + "] is used by multiple types. Set update_all_types to true to update "
                            + "[fielddata_frequency_filter.min_segment_size] across all types.");
                }
            }
        }

        public boolean fielddata() {
            return fielddata;
        }

        public void setFielddata(boolean fielddata) {
            checkIfFrozen();
            this.fielddata = fielddata;
        }

        public double fielddataMinFrequency() {
            return fielddataMinFrequency;
        }

        public void setFielddataMinFrequency(double fielddataMinFrequency) {
            checkIfFrozen();
            this.fielddataMinFrequency = fielddataMinFrequency;
        }

        public double fielddataMaxFrequency() {
            return fielddataMaxFrequency;
        }

        public void setFielddataMaxFrequency(double fielddataMaxFrequency) {
            checkIfFrozen();
            this.fielddataMaxFrequency = fielddataMaxFrequency;
        }

        public int fielddataMinSegmentSize() {
            return fielddataMinSegmentSize;
        }

        public void setFielddataMinSegmentSize(int fielddataMinSegmentSize) {
            checkIfFrozen();
            this.fielddataMinSegmentSize = fielddataMinSegmentSize;
        }

        void setPrefixFieldType(PrefixFieldType prefixFieldType) {
            checkIfFrozen();
            this.prefixFieldType = prefixFieldType;
        }

        void setIndexPhrases(boolean indexPhrases) {
            checkIfFrozen();
            this.indexPhrases = indexPhrases;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            if (prefixFieldType == null || prefixFieldType.accept(value.length()) == false) {
                return super.prefixQuery(value, method, context);
            }
            Query tq = prefixFieldType.termQuery(value, context);
            if (method == null || method == MultiTermQuery.CONSTANT_SCORE_REWRITE
                || method == MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE) {
                return new ConstantScoreQuery(tq);
            }
            return tq;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (omitNorms()) {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            } else {
                return new NormsFieldExistsQuery(name());
            }
        }

        @Override
        public Query nullValueQuery() {
            if (nullValue() == null) {
                return null;
            }
            return termQuery(nullValue(), null);
        }

        @Override
        public Query phraseQuery(String field, TokenStream stream, int slop, boolean enablePosIncrements) throws IOException {
            if (indexPhrases && slop == 0 && hasGaps(cache(stream)) == false) {
                stream = new FixedShingleFilter(stream, 2);
                field = field + FAST_PHRASE_SUFFIX;
            }
            return super.phraseQuery(field, stream, slop, enablePosIncrements);
        }

        @Override
        public Query multiPhraseQuery(String field, TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
            if (indexPhrases && slop == 0 && hasGaps(cache(stream)) == false) {
                stream = new FixedShingleFilter(stream, 2);
                field = field + FAST_PHRASE_SUFFIX;
            }
            return super.multiPhraseQuery(field, stream, slop, enablePositionIncrements);
        }

        private static CachingTokenFilter cache(TokenStream in) {
            if (in instanceof CachingTokenFilter) {
                return (CachingTokenFilter) in;
            }
            return new CachingTokenFilter(in);
        }

        private static boolean hasGaps(CachingTokenFilter stream) throws IOException {
            PositionIncrementAttribute posIncAtt = stream.getAttribute(PositionIncrementAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                if (posIncAtt.getPositionIncrement() > 1) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            if (fielddata == false) {
                throw new IllegalArgumentException("Fielddata is disabled on text fields by default. Set fielddata=true on [" + name()
                        + "] in order to load fielddata in memory by uninverting the inverted index. Note that this can however "
                                + "use significant memory. Alternatively use a keyword field instead.");
            }
            return new PagedBytesIndexFieldData.Builder(fielddataMinFrequency, fielddataMaxFrequency, fielddataMinSegmentSize);
        }

    }

    private int positionIncrementGap;
    private PrefixFieldMapper prefixFieldMapper;
    private PhraseFieldMapper phraseFieldMapper;

    protected TextFieldMapper(String simpleName,
                              Integer position,
                              TextFieldType fieldType,
                              MappedFieldType defaultFieldType,
                              int positionIncrementGap,
                              PrefixFieldMapper prefixFieldMapper,
                              Settings indexSettings,
                              MultiFields multiFields,
                              CopyTo copyTo) {
        super(simpleName, position, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        assert fieldType.tokenized();
        assert fieldType.hasDocValues() == false;
        if (fieldType().indexOptions() == IndexOptions.NONE && fieldType().fielddata()) {
            throw new IllegalArgumentException("Cannot enable fielddata on a [text] field that is not indexed: [" + name() + "]");
        }
        this.positionIncrementGap = positionIncrementGap;
        this.prefixFieldMapper = prefixFieldMapper;
        this.phraseFieldMapper = fieldType.indexPhrases ? new PhraseFieldMapper(new PhraseFieldType(fieldType), indexSettings) : null;
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

        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            Field field = new Field(fieldType().name(), value, fieldType());
            fields.add(field);
            if (fieldType().omitNorms()) {
                createFieldNamesField(context, fields);
            }
            if (prefixFieldMapper != null) {
                prefixFieldMapper.addField(value, fields);
            }
            if (phraseFieldMapper != null) {
                fields.add(new Field(phraseFieldMapper.fieldType.name(), value, phraseFieldMapper.fieldType));
            }
        }
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> subIterators = new ArrayList<>();
        if (prefixFieldMapper != null) {
            subIterators.add(prefixFieldMapper);
        }
        if (phraseFieldMapper != null) {
            subIterators.add(phraseFieldMapper);
        }
        if (subIterators.size() == 0) {
            return super.iterator();
        }
        return Iterators.concat(super.iterator(), subIterators.iterator());
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);
        TextFieldMapper mw = (TextFieldMapper) mergeWith;
        if (this.prefixFieldMapper != null && mw.prefixFieldMapper != null) {
            this.prefixFieldMapper = (PrefixFieldMapper) this.prefixFieldMapper.merge(mw.prefixFieldMapper, updateAllTypes);
        }
        else if (this.prefixFieldMapper != null || mw.prefixFieldMapper != null) {
            throw new IllegalArgumentException("mapper [" + name() + "] has different index_prefix settings, current ["
                + this.prefixFieldMapper + "], merged [" + mw.prefixFieldMapper + "]");
        }
        else if (this.fieldType().indexPhrases != mw.fieldType().indexPhrases) {
            throw new IllegalArgumentException("mapper [" + name() + "] has different index_phrases settings, current ["
                + this.fieldType().indexPhrases + "], merged [" + mw.fieldType().indexPhrases + "]");
        }
    }

    @Override
    public TextFieldType fieldType() {
        return (TextFieldType) super.fieldType();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        doXContentAnalyzers(builder, includeDefaults);

        if (includeDefaults || positionIncrementGap != POSITION_INCREMENT_GAP_USE_ANALYZER) {
            builder.field("position_increment_gap", positionIncrementGap);
        }

        if (includeDefaults || fieldType().fielddata() != ((TextFieldType) defaultFieldType).fielddata()) {
            builder.field("fielddata", fieldType().fielddata());
        }
        if (fieldType().fielddata()) {
            if (includeDefaults
                    || fieldType().fielddataMinFrequency() != Defaults.FIELDDATA_MIN_FREQUENCY
                    || fieldType().fielddataMaxFrequency() != Defaults.FIELDDATA_MAX_FREQUENCY
                    || fieldType().fielddataMinSegmentSize() != Defaults.FIELDDATA_MIN_SEGMENT_SIZE) {
                builder.startObject("fielddata_frequency_filter");
                if (includeDefaults || fieldType().fielddataMinFrequency() != Defaults.FIELDDATA_MIN_FREQUENCY) {
                    builder.field("min", fieldType().fielddataMinFrequency());
                }
                if (includeDefaults || fieldType().fielddataMaxFrequency() != Defaults.FIELDDATA_MAX_FREQUENCY) {
                    builder.field("max", fieldType().fielddataMaxFrequency());
                }
                if (includeDefaults || fieldType().fielddataMinSegmentSize() != Defaults.FIELDDATA_MIN_SEGMENT_SIZE) {
                    builder.field("min_segment_size", fieldType().fielddataMinSegmentSize());
                }
                builder.endObject();
            }
        }
        if (fieldType().prefixFieldType != null) {
            fieldType().prefixFieldType.doXContent(builder);
        }
        if (fieldType().indexPhrases) {
            builder.field("index_phrases", fieldType().indexPhrases);
        }
    }
}
