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
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.joda.time.DateTimeZone;

/**
 * This defines the core properties and functions to operate on a field.
 */
public abstract class MappedFieldType {

    private final String name;
    private final boolean docValues;
    private final boolean isIndexed;
    private float boost;
    private NamedAnalyzer indexAnalyzer;
    private NamedAnalyzer searchAnalyzer;
    private NamedAnalyzer searchQuoteAnalyzer;
    protected boolean hasPositions;
    private boolean eagerGlobalOrdinals;

    protected MappedFieldType(MappedFieldType ref) {
        this.name = ref.name();
        this.boost = ref.boost();
        this.isIndexed = ref.isIndexed;
        this.docValues = ref.hasDocValues();
        this.indexAnalyzer = ref.indexAnalyzer();
        this.searchAnalyzer = ref.searchAnalyzer();
        this.searchQuoteAnalyzer = ref.searchQuoteAnalyzer();
        this.eagerGlobalOrdinals = ref.eagerGlobalOrdinals;
        this.hasPositions = ref.hasPositions;
    }

    public MappedFieldType(String name, boolean isIndexed, boolean hasDocValues) {
        setBoost(1.0f);
        this.name = Objects.requireNonNull(name);
        this.isIndexed = isIndexed;
        this.docValues = hasDocValues;
    }

    @Override
    public abstract MappedFieldType clone();

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MappedFieldType fieldType = (MappedFieldType) o;

        return boost == fieldType.boost &&
            docValues == fieldType.docValues &&
            Objects.equals(name, fieldType.name) &&
            Objects.equals(indexAnalyzer, fieldType.indexAnalyzer) &&
            Objects.equals(searchAnalyzer, fieldType.searchAnalyzer) &&
            Objects.equals(searchQuoteAnalyzer(), fieldType.searchQuoteAnalyzer()) &&
            Objects.equals(eagerGlobalOrdinals, fieldType.eagerGlobalOrdinals);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            name,
            boost,
            docValues,
            indexAnalyzer,
            searchAnalyzer,
            searchQuoteAnalyzer,
            eagerGlobalOrdinals
        );
    }

    /** Returns the name of this type, as would be specified in mapping properties */
    public abstract String typeName();

    public String name() {
        return name;
    }

    public float boost() {
        return boost;
    }

    public void setBoost(float boost) {
        this.boost = boost;
    }

    public boolean hasPositions() {
        return hasPositions;
    }

    public boolean hasDocValues() {
        return docValues;
    }

    public NamedAnalyzer indexAnalyzer() {
        return indexAnalyzer;
    }

    public void setIndexAnalyzer(NamedAnalyzer analyzer) {
        this.indexAnalyzer = analyzer;
    }

    public NamedAnalyzer searchAnalyzer() {
        return searchAnalyzer;
    }

    public void setSearchAnalyzer(NamedAnalyzer analyzer) {
        this.searchAnalyzer = analyzer;
    }

    public NamedAnalyzer searchQuoteAnalyzer() {
        return searchQuoteAnalyzer == null ? searchAnalyzer : searchQuoteAnalyzer;
    }

    public void setSearchQuoteAnalyzer(NamedAnalyzer analyzer) {
        this.searchQuoteAnalyzer = analyzer;
    }


    /** Given a value that comes from the stored fields API, convert it to the
     *  expected type. For instance a date field would store dates as longs and
     *  format it back to a string in this method. */
    public Object valueForDisplay(Object value) {
        return value;
    }

    /**
     * Returns true if the field is searchable.
     */
    public boolean isSearchable() {
        return isIndexed;
    }


    /** Generates a query that will only match documents that contain the given value.
     *  The default implementation returns a {@link TermQuery} over the value bytes,
     *  boosted by {@link #boost()}.
     *  @throws IllegalArgumentException if {@code value} cannot be converted to the expected data type or if the field is not searchable
     *      due to the way it is configured (eg. not indexed)
     *  @throws ElasticsearchParseException if {@code value} cannot be converted to the expected data type
     *  @throws UnsupportedOperationException if the field is not searchable regardless of options
     *  @throws QueryShardException if the field is not searchable regardless of options
     */
    // TODO: Standardize exception types
    public abstract Query termQuery(Object value, @Nullable QueryShardContext context);

    /** Build a constant-scoring query that matches all values. The default implementation uses a
     * {@link ConstantScoreQuery} around a {@link BooleanQuery} whose {@link Occur#SHOULD} clauses
     * are generated with {@link #termQuery}. */
    public Query termsQuery(List<?> values, @Nullable QueryShardContext context) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Object value : values) {
            builder.add(termQuery(value, context), Occur.SHOULD);
        }
        return new ConstantScoreQuery(builder.build());
    }

    /**
     * Factory method for range queries.
     * @param relation the relation, nulls should be interpreted like INTERSECTS
     */
    public Query rangeQuery(Object lowerTerm,
                            Object upperTerm,
                            boolean includeLower,
                            boolean includeUpper,
                            ShapeRelation relation,
                            DateTimeZone timeZone,
                            QueryShardContext context) {
        throw new IllegalArgumentException("Field [" + name + "] of type [" + typeName() + "] does not support range queries");
    }

    public Query fuzzyQuery(Object value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
        throw new IllegalArgumentException("Can only use fuzzy queries on keyword and text fields - not on [" + name + "] which is of type [" + typeName() + "]");
    }

    public Query prefixQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, QueryShardContext context) {
        throw new QueryShardException(context, "Can only use prefix queries on keyword and text fields - not on [" + name + "] which is of type [" + typeName() + "]");
    }

    public abstract Query existsQuery(QueryShardContext context);

    /** A term query to use when parsing a query string. Can return {@code null}. */
    @Nullable
    public Query queryStringTermQuery(Term term) {
        return null;
    }

    /** @throws IllegalArgumentException if the fielddata is not supported on this type.
     *  An IllegalArgumentException is needed in order to return an http error 400
     *  when this error occurs in a request. see: {@link org.elasticsearch.ExceptionsHelper#status}
     **/
    protected final void failIfNoDocValues() {
        if (hasDocValues() == false) {
            throw new IllegalArgumentException("Can't load fielddata on [" + name()
                + "] because fielddata is unsupported on fields of type ["
                + typeName() + "]. Use doc values instead.");
        }
    }

    protected final void failIfNotIndexed() {
        if (isIndexed == false) {
            // we throw an IAE rather than an ISE so that it translates to a 4xx code rather than 5xx code on the http layer
            throw new IllegalArgumentException("Cannot search on field [" + name() + "] since it is not indexed.");
        }
    }
}
