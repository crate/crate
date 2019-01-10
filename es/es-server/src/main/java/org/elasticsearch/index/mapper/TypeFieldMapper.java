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

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.ConstantIndexFieldData;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class TypeFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_type";

    public static final String CONTENT_TYPE = "_type";

    public static class Defaults {
        public static final String NAME = TypeFieldMapper.NAME;

        public static final MappedFieldType FIELD_TYPE = new TypeFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setName(NAME);
            FIELD_TYPE.freeze();
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            throw new MapperParsingException(NAME + " is not configurable");
        }

        @Override
        public MetadataFieldMapper getDefault(MappedFieldType fieldType, ParserContext context) {
            final IndexSettings indexSettings = context.mapperService().getIndexSettings();
            return new TypeFieldMapper(indexSettings, fieldType);
        }
    }

    static final class TypeFieldType extends StringFieldType {

        private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(LogManager.getLogger(TypeFieldType.class));

        TypeFieldType() {
        }

        protected TypeFieldType(TypeFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new TypeFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            if (hasDocValues()) {
                return new DocValuesIndexFieldData.Builder();
            } else {
                // means the index has a single type and the type field is implicit
                Function<MapperService, String> typeFunction = mapperService -> {
                    Collection<String> types = mapperService.types();
                    if (types.size() > 1) {
                        throw new AssertionError();
                    }
                    // If we reach here, there is necessarily one type since we were able to find a `_type` field
                    String type = types.iterator().next();
                    return type;
                };
                return new ConstantIndexFieldData.Builder(typeFunction);
            }
        }

        @Override
        public boolean isSearchable() {
            return true;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new MatchAllDocsQuery();
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            return termsQuery(Arrays.asList(value), context);
        }

        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            if (context.getIndexSettings().isSingleType()) {
                Collection<String> indexTypes = context.getMapperService().types();
                if (indexTypes.isEmpty()) {
                    return new MatchNoDocsQuery("No types");
                }
                assert indexTypes.size() == 1;
                BytesRef indexType = indexedValueForSearch(indexTypes.iterator().next());
                if (values.stream()
                        .map(this::indexedValueForSearch)
                        .anyMatch(indexType::equals)) {
                    if (context.getMapperService().hasNested()) {
                        // type filters are expected not to match nested docs
                        return Queries.newNonNestedFilter(context.indexVersionCreated());
                    } else {
                        return new MatchAllDocsQuery();
                    }
                } else {
                    return new MatchNoDocsQuery("Type list does not contain the index type");
                }
            } else {
                if (indexOptions() == IndexOptions.NONE) {
                    throw new AssertionError();
                }
                final BytesRef[] types = values.stream()
                        .map(this::indexedValueForSearch)
                        .toArray(size -> new BytesRef[size]);
                return new TypesQuery(types);
            }
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
            if (context.getIndexSettings().isSingleType() == false) {
                return new TermRangeQuery(name(), lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
                        upperTerm == null ? null : indexedValueForSearch(upperTerm), includeLower, includeUpper);
            } else {
                // this means the index has a single type and the type field is implicit
                DEPRECATION_LOGGER.deprecatedAndMaybeLog("range_single_type",
                        "Running [range] query on [_type] field for an index with a single type. As types are deprecated, this functionality will be removed in future releases.");
                Collection<String> types = context.getMapperService().types();
                String type = types.iterator().hasNext() ? types.iterator().next() : null;
                if (type != null) {
                    Query result = new MatchAllDocsQuery();
                    BytesRef typeBytes = new BytesRef(type);
                    if (lowerTerm != null) {
                        int comp = indexedValueForSearch(lowerTerm).compareTo(typeBytes);
                        if (comp > 0 || (comp == 0 && includeLower == false)) {
                            result = new MatchNoDocsQuery("[_type] was lexicographically smaller than lower bound of range");
                        }
                    }
                    if (upperTerm != null) {
                        int comp = indexedValueForSearch(upperTerm).compareTo(typeBytes);
                        if (comp < 0 || (comp == 0 && includeUpper == false)) {
                            result = new MatchNoDocsQuery("[_type] was lexicographically greater than upper bound of range");
                        }
                    }
                    return result;
                } else {
                    return new MatchNoDocsQuery();
                }
            }
        }
    }

    /**
     * Specialization for a disjunction over many _type
     */
    public static class TypesQuery extends Query {
        // Same threshold as TermInSetQuery
        private static final int BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD = 16;

        private final BytesRef[] types;

        public TypesQuery(BytesRef... types) {
            if (types == null) {
                throw new NullPointerException("types cannot be null.");
            }
            if (types.length == 0) {
                throw new IllegalArgumentException("types must contains at least one value.");
            }
            this.types = types;
        }

        public BytesRef[] getTerms() {
            return types;
        }

        @Override
        public Query rewrite(IndexReader reader) throws IOException {
            final int threshold = Math.min(BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD, BooleanQuery.getMaxClauseCount());
            if (types.length <= threshold) {
                Set<BytesRef> uniqueTypes = new HashSet<>();
                BooleanQuery.Builder bq = new BooleanQuery.Builder();
                int totalDocFreq = 0;
                for (BytesRef type : types) {
                    if (uniqueTypes.add(type)) {
                        Term term = new Term(CONTENT_TYPE, type);
                        TermContext context = TermContext.build(reader.getContext(), term);
                        if (context.docFreq() == 0) {
                            // this _type is not present in the reader
                            continue;
                        }
                        totalDocFreq += context.docFreq();
                        // strict equality should be enough ?
                        if (totalDocFreq >= reader.maxDoc()) {
                            assert totalDocFreq == reader.maxDoc();
                            // Matches all docs since _type is a single value field
                            // Using a match_all query will help Lucene perform some optimizations
                            // For instance, match_all queries as filter clauses are automatically removed
                            return new MatchAllDocsQuery();
                        }
                        bq.add(new TermQuery(term, context), BooleanClause.Occur.SHOULD);
                    }
                }
                return new ConstantScoreQuery(bq.build());
            }
            return new TermInSetQuery(CONTENT_TYPE, types);
        }

        @Override
        public boolean equals(Object obj) {
            if (sameClassAs(obj) == false) {
                return false;
            }
            TypesQuery that = (TypesQuery) obj;
            return Arrays.equals(types, that.types);
        }

        @Override
        public int hashCode() {
            return 31 * classHash() + Arrays.hashCode(types);
        }

        @Override
        public String toString(String field) {
            StringBuilder builder = new StringBuilder();
            for (BytesRef type : types) {
                if (builder.length() > 0) {
                    builder.append(' ');
                }
                builder.append(new Term(CONTENT_TYPE, type).toString());
            }
            return builder.toString();
        }
    }

    private TypeFieldMapper(IndexSettings indexSettings, MappedFieldType existing) {
        this(existing == null ? defaultFieldType(indexSettings) : existing.clone(),
             indexSettings);
    }

    private TypeFieldMapper(MappedFieldType fieldType, IndexSettings indexSettings) {
        super(NAME, fieldType, defaultFieldType(indexSettings), indexSettings.getSettings());
    }

    private static MappedFieldType defaultFieldType(IndexSettings indexSettings) {
        MappedFieldType defaultFieldType = Defaults.FIELD_TYPE.clone();
        if (indexSettings.isSingleType()) {
            defaultFieldType.setIndexOptions(IndexOptions.NONE);
            defaultFieldType.setHasDocValues(false);
        } else {
            defaultFieldType.setIndexOptions(IndexOptions.DOCS);
            defaultFieldType.setHasDocValues(true);
        }
        return defaultFieldType;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // we parse in pre parse
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        if (fieldType().indexOptions() == IndexOptions.NONE && !fieldType().stored()) {
            return;
        }
        fields.add(new Field(fieldType().name(), context.sourceToParse().type(), fieldType()));
        if (fieldType().hasDocValues()) {
            fields.add(new SortedSetDocValuesField(fieldType().name(), new BytesRef(context.sourceToParse().type())));
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        // do nothing here, no merging, but also no exception
    }
}
