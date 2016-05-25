/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.List;

class ArrayFieldType extends MappedFieldType {

    private final MappedFieldType innerFieldType;

    private ArrayFieldType(ArrayFieldType ref) {
        super(ref);
        this.innerFieldType = ref.innerFieldType;
    }

    ArrayFieldType(MappedFieldType innerFieldType) {
        this.innerFieldType = innerFieldType;
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @Override
    public MappedFieldType clone() {
        return new ArrayFieldType(this);
    }

    @Override
    public String typeName() {
        return ArrayMapper.CONTENT_TYPE;
    }

    @Override
    public void checkCompatibility(MappedFieldType other, List<String> conflicts, boolean strict) {
        super.checkCompatibility(other, conflicts, strict);
        if (other instanceof ArrayFieldType) {
            innerFieldType.checkCompatibility(((ArrayFieldType) other).innerFieldType, conflicts, strict);
        }
    }

    @Override
    public boolean isNumeric() {
        return innerFieldType.isNumeric();
    }

    @Override
    public boolean isSortable() {
        return innerFieldType.isSortable();
    }

    @Override
    public FieldDataType fieldDataType() {
        return innerFieldType.fieldDataType();
    }

    @Override
    public void setFieldDataType(FieldDataType fieldDataType) {
        innerFieldType.setFieldDataType(fieldDataType);
    }

    @Override
    public boolean hasDocValues() {
        return innerFieldType.hasDocValues();
    }

    @Override
    public void setHasDocValues(boolean hasDocValues) {
        innerFieldType.setHasDocValues(hasDocValues);
    }

    @Override
    public Loading normsLoading() {
        return innerFieldType.normsLoading();
    }

    @Override
    public void setNormsLoading(Loading normsLoading) {
        innerFieldType.setNormsLoading(normsLoading);
    }

    @Override
    public NamedAnalyzer indexAnalyzer() {
        return innerFieldType.indexAnalyzer();
    }

    @Override
    public void setIndexAnalyzer(NamedAnalyzer analyzer) {
        innerFieldType.setIndexAnalyzer(analyzer);
    }

    @Override
    public NamedAnalyzer searchAnalyzer() {
        return innerFieldType.searchAnalyzer();
    }

    @Override
    public void setSearchAnalyzer(NamedAnalyzer analyzer) {
        innerFieldType.setSearchAnalyzer(analyzer);
    }

    @Override
    public NamedAnalyzer searchQuoteAnalyzer() {
        return innerFieldType.searchQuoteAnalyzer();
    }

    @Override
    public void setSearchQuoteAnalyzer(NamedAnalyzer analyzer) {
        innerFieldType.setSearchQuoteAnalyzer(analyzer);
    }

    @Override
    public SimilarityProvider similarity() {
        return innerFieldType.similarity();
    }

    @Override
    public void setSimilarity(SimilarityProvider similarity) {
        innerFieldType.setSimilarity(similarity);
    }

    @Override
    public Object nullValue() {
        return innerFieldType.nullValue();
    }

    @Override
    public String nullValueAsString() {
        return innerFieldType.nullValueAsString();
    }

    @Override
    public void setNullValue(Object nullValue) {
        innerFieldType.setNullValue(nullValue);
    }

    @Override
    public Object value(Object value) {
        return innerFieldType.value(value);
    }

    @Override
    public Object valueForSearch(Object value) {
        return innerFieldType.valueForSearch(value);
    }

    @Override
    public BytesRef indexedValueForSearch(Object value) {
        return innerFieldType.indexedValueForSearch(value);
    }

    @Override
    public boolean useTermQueryWithQueryString() {
        return innerFieldType.useTermQueryWithQueryString();
    }

    @Override
    public Term createTerm(Object value) {
        return innerFieldType.createTerm(value);
    }

    @Override
    public Query termQuery(Object value, @Nullable QueryParseContext context) {
        return innerFieldType.termQuery(value, context);
    }

    @Override
    public Query termsQuery(List values, @Nullable QueryParseContext context) {
        return innerFieldType.termsQuery(values, context);
    }

    @Override
    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper) {
        return innerFieldType.rangeQuery(lowerTerm, upperTerm, includeLower, includeUpper);
    }

    @Override
    public Query fuzzyQuery(Object value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
        return innerFieldType.fuzzyQuery(value, fuzziness, prefixLength, maxExpansions, transpositions);
    }

    @Override
    public Query prefixQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context) {
        return innerFieldType.prefixQuery(value, method, context);
    }

    @Override
    public Query regexpQuery(String value, int flags, int maxDeterminizedStates, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context) {
        return innerFieldType.regexpQuery(value, flags, maxDeterminizedStates, method, context);
    }

    @Override
    public Query nullValueQuery() {
        return innerFieldType.nullValueQuery();
    }

    @Override
    public FieldStats stats(Terms terms, int maxDoc) throws IOException {
        return innerFieldType.stats(terms, maxDoc);
    }

    @Override
    @Nullable
    public Query queryStringTermQuery(Term term) {
        return innerFieldType.queryStringTermQuery(term);
    }
}
