/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.lucene;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import io.crate.analyze.WhereClause;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.lucene.match.MatchQueryBuilder;
import io.crate.lucene.match.MultiMatchQueryBuilder;
import io.crate.metadata.DocReferenceConverter;
import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.collect.LuceneDocCollector;
import io.crate.operation.operator.*;
import io.crate.operation.operator.any.*;
import io.crate.operation.predicate.IsNullPredicate;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.operation.predicate.NotPredicate;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.LuceneDocLevelReferenceResolver;
import io.crate.operation.scalar.geo.DistanceFunction;
import io.crate.operation.scalar.geo.WithinFunction;
import io.crate.planner.symbol.*;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.sandbox.queries.regex.JavaUtilRegexCapabilities;
import org.apache.lucene.sandbox.queries.regex.RegexQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.docset.MatchDocIdSet;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.query.RegexpFlag;
import org.elasticsearch.index.search.geo.GeoDistanceRangeFilter;
import org.elasticsearch.index.search.geo.GeoPolygonFilter;
import org.elasticsearch.index.search.geo.InMemoryGeoBoundingBoxFilter;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static io.crate.operation.scalar.regex.RegexMatcher.isPcrePattern;

@Singleton
public class LuceneQueryBuilder {

    private final static Visitor VISITOR = new Visitor();
    private final CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor;

    @Inject
    public LuceneQueryBuilder(Functions functions) {
        inputSymbolVisitor = new CollectInputSymbolVisitor<>(functions, new LuceneDocLevelReferenceResolver(null));
    }

    public Context convert(WhereClause whereClause, SearchContext searchContext, IndexCache indexCache) throws UnsupportedFeatureException {
        Context ctx = new Context(inputSymbolVisitor, searchContext, indexCache);
        if (whereClause.noMatch()) {
            ctx.query = Queries.newMatchNoDocsQuery();
        } else if (!whereClause.hasQuery()) {
            ctx.query = Queries.newMatchAllQuery();
        } else {
            ctx.query = VISITOR.process(whereClause.query(), ctx);
        }
        return ctx;
    }

    public static class Context {
        Query query;

        final Map<String, Object> filteredFieldValues = new HashMap<>();

        final SearchContext searchContext;
        final CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor;
        final IndexCache indexCache;

        Context(CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor,
                SearchContext searchContext,
                IndexCache indexCache) {
            this.inputSymbolVisitor = inputSymbolVisitor;
            this.searchContext = searchContext;
            this.indexCache = indexCache;
        }

        public Query query() {
            return this.query;
        }

        @Nullable
        public Float minScore() {
            Object score = filteredFieldValues.get("_score");
            if (score == null) {
                return null;
            }
            return ((Number) score).floatValue();
        }

        @Nullable
        public String unsupportedMessage(String field){
            return UNSUPPORTED_FIELDS.get(field);
        }

        /**
         * These fields are ignored in the whereClause.
         * If a filtered field is encountered the value of the literal is written into filteredFieldValues
         * (only applies to Function with 2 arguments and if left == reference and right == literal)
         */
        final static Set<String> FILTERED_FIELDS = new HashSet<String>(){{ add("_score"); }};

        /**
         * key = columnName
         * value = error message
         * <p/>
         * (in the _version case if the primary key is present a GetPlan is built from the planner and
         * the LuceneQueryBuilder is never used)
         */
        final static Map<String, String> UNSUPPORTED_FIELDS = ImmutableMap.<String, String>builder()
                .put("_version", "\"_version\" column is not valid in the WHERE clause")
                .build();
    }

    public static String convertWildcardToRegex(String wildcardString) {
        // lucene uses * and ? as wildcard characters
        // but via SQL they are used as % and _
        // here they are converted back.
        wildcardString = wildcardString.replaceAll("(?<!\\\\)\\*", "\\\\*");
        wildcardString = wildcardString.replaceAll("(?<!\\\\)%", ".*");
        wildcardString = wildcardString.replaceAll("\\\\%", "%");

        wildcardString = wildcardString.replaceAll("(?<!\\\\)\\?", "\\\\?");
        wildcardString = wildcardString.replaceAll("(?<!\\\\)_", ".");
        return wildcardString.replaceAll("\\\\_", "_");
    }

    public static String convertWildcard(String wildcardString) {
        // lucene uses * and ? as wildcard characters
        // but via SQL they are used as % and _
        // here they are converted back.
        wildcardString = wildcardString.replaceAll("(?<!\\\\)\\*", "\\\\*");
        wildcardString = wildcardString.replaceAll("(?<!\\\\)%", "*");
        wildcardString = wildcardString.replaceAll("\\\\%", "%");

        wildcardString = wildcardString.replaceAll("(?<!\\\\)\\?", "\\\\?");
        wildcardString = wildcardString.replaceAll("(?<!\\\\)_", "?");
        return wildcardString.replaceAll("\\\\_", "_");
    }

    public static String negateWildcard(String wildCard) {
        return String.format(Locale.ENGLISH, "~(%s)", wildCard);
    }

    static class Visitor extends SymbolVisitor<Context, Query> {

        interface FunctionToQuery {

            @Nullable
            Query apply (Function input, Context context) throws IOException;
        }

        static abstract class CmpQuery implements FunctionToQuery {

            @Nullable
            protected Tuple<Reference, Literal> prepare(Function input) {
                assert input != null;
                assert input.arguments().size() == 2;

                Symbol left = input.arguments().get(0);
                Symbol right = input.arguments().get(1);

                if (!(left instanceof Reference) || !(right.symbolType().isValueSymbol())) {
                    return null;
                }
                assert right.symbolType() == SymbolType.LITERAL;
                return new Tuple<>((Reference)left, (Literal)right);
            }
        }

        static abstract class AbstractAnyQuery implements FunctionToQuery {

            @Override
            public Query apply(Function function, Context context) throws IOException {
                Symbol left = function.arguments().get(0);
                Symbol collectionSymbol = function.arguments().get(1);
                Preconditions.checkArgument(DataTypes.isCollectionType(collectionSymbol.valueType()),
                        "invalid argument for ANY expression");
                if (left.symbolType().isValueSymbol()) {
                    // 1 = any (array_col) - simple eq
                    assert collectionSymbol.symbolType().isReference() : "no reference found in ANY expression";
                    return applyArrayReference((Reference)collectionSymbol, (Literal)left, context);
                } else if (collectionSymbol.symbolType().isValueSymbol()) {
                    assert left.symbolType().isReference() : "no reference found in ANY expression";
                    return applyArrayLiteral((Reference)left, (Literal)collectionSymbol, context);
                } else {
                    // should never get here - 2 literal arguments must have been normalized away yet
                    return null;
                }
            }

            /**
             * converts Strings to BytesRef on the fly
             */
            public static Iterable<?> toIterable(Object value) {
                return Iterables.transform(AnyOperator.collectionValueToIterable(value), new com.google.common.base.Function<Object, Object>() {
                    @javax.annotation.Nullable
                    @Override
                    public Object apply(@javax.annotation.Nullable Object input) {
                        if (input != null && input instanceof String) {
                            input = new BytesRef((String)input);
                        }
                        return input;
                    }
                });
            }

            protected abstract Query applyArrayReference(Reference arrayReference, Literal literal, Context context) throws IOException;
            protected abstract Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) throws IOException;
        }

        static class AnyEqQuery extends AbstractAnyQuery {

            private TermsFilter termsFilter(String columnName, Literal arrayLiteral) {
                TermsFilter termsFilter;
                Object values = arrayLiteral.value();
                if (values instanceof Collection) {
                    termsFilter = new TermsFilter(
                            columnName,
                            getBytesRefs((Collection)values,
                                    TermBuilder.forType(arrayLiteral.valueType())));
                } else  {
                    termsFilter = new TermsFilter(
                            columnName,
                            getBytesRefs((Object[])values,
                                    TermBuilder.forType(arrayLiteral.valueType())));
                }
                return termsFilter;
            }

            @Override
            protected Query applyArrayReference(Reference arrayReference, Literal literal, Context context) throws IOException {
                QueryBuilderHelper builder = QueryBuilderHelper.forType(((CollectionType)arrayReference.valueType()).innerType());
                return builder.eq(arrayReference.ident().columnIdent().fqn(), literal.value());
            }

            @Override
            protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) throws IOException {
                String columnName = reference.ident().columnIdent().fqn();
                return new FilteredQuery(Queries.newMatchAllQuery(), termsFilter(columnName, arrayLiteral));
            }
        }

        static class AnyNeqQuery extends AbstractAnyQuery {

            @Override
            protected Query applyArrayReference(Reference arrayReference, Literal literal, Context context) throws IOException {
                // 1 != any ( col ) -->  gt 1 or lt 1
                String columnName = arrayReference.info().ident().columnIdent().fqn();
                Object value = literal.value();

                QueryBuilderHelper builder = QueryBuilderHelper.forType(arrayReference.valueType());
                BooleanQuery query = new BooleanQuery();
                query.setMinimumNumberShouldMatch(1);
                query.add(
                        builder.rangeQuery(columnName, value, null, false, false),
                        BooleanClause.Occur.SHOULD
                );
                query.add(
                        builder.rangeQuery(columnName, null, value, false, false),
                        BooleanClause.Occur.SHOULD
                );
                return query;
            }

            @Override
            protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) throws IOException {
                //  col != ANY ([1,2,3]) --> not(col=1 and col=2 and col=3)
                String columnName = reference.info().ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(reference.valueType());
                BooleanFilter filter = new BooleanFilter();

                BooleanFilter notFilter = new BooleanFilter();
                for (Object value : toIterable(arrayLiteral.value())) {
                    notFilter.add(builder.eqFilter(columnName, value), BooleanClause.Occur.MUST);
                }
                filter.add(notFilter, BooleanClause.Occur.MUST_NOT);

                return new FilteredQuery(Queries.newMatchAllQuery(), filter);
            }
        }

        static class AnyNotLikeQuery extends AbstractAnyQuery {

            @Override
            protected Query applyArrayReference(Reference arrayReference, Literal literal, Context context) throws IOException {
                String notLike = negateWildcard(
                        convertWildcardToRegex(BytesRefs.toString(literal.value())));
                return new RegexpQuery(new Term(
                        arrayReference.info().ident().columnIdent().fqn(),
                        notLike),
                        RegexpFlag.COMPLEMENT.value()
                );
            }

            @Override
            protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) throws IOException {
                // col not like ANY (['a', 'b']) --> not(and(like(col, 'a'), like(col, 'b')))
                BooleanQuery query = new BooleanQuery();
                BooleanQuery notQuery = new BooleanQuery();

                String columnName = reference.ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(reference.valueType());
                for (Object value : toIterable(arrayLiteral.value())) {
                    notQuery.add(builder.like(columnName, value), BooleanClause.Occur.MUST);
                }
                query.add(notQuery, BooleanClause.Occur.MUST_NOT);
                return query;
            }
        }

        static class AnyLikeQuery extends AbstractAnyQuery {

            private final LikeQuery likeQuery = new LikeQuery();

            @Override
            protected Query applyArrayReference(Reference arrayReference, Literal literal, Context context) throws IOException {
                return likeQuery.toQuery(arrayReference, literal.value());
            }

            @Override
            protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) throws IOException {
                // col like ANY (['a', 'b']) --> or(like(col, 'a'), like(col, 'b'))
                BooleanQuery booleanQuery = new BooleanQuery();
                booleanQuery.setMinimumNumberShouldMatch(1);
                for (Object value : toIterable(arrayLiteral.value())) {
                    booleanQuery.add(likeQuery.toQuery(reference, value), BooleanClause.Occur.SHOULD);
                }
                return booleanQuery;
            }
        }

        static class LikeQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> tuple = prepare(input);
                if (tuple == null) {
                    return null;
                }
                return toQuery(tuple.v1(), tuple.v2().value());
            }

            public Query toQuery(Reference reference, Object value) {
                String columnName = reference.info().ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(reference.valueType());
                return builder.like(columnName, value);
            }
        }

        static class InQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) throws IOException {
                Tuple<Reference, Literal> tuple = prepare(input);
                if (tuple == null) {
                    return null;
                }
                String field = tuple.v1().info().ident().columnIdent().fqn();
                Literal literal = tuple.v2();
                CollectionType dataType = ((CollectionType) literal.valueType());

                Set values = (Set) literal.value();
                DataType innerType = dataType.innerType();
                BytesRef[] terms = getBytesRefs(values, TermBuilder.forType(innerType));
                TermsFilter termsFilter = new TermsFilter(field, terms);
                return new FilteredQuery(Queries.newMatchAllQuery(), termsFilter);
            }
        }

        class NotQuery implements FunctionToQuery {

            @Override
            public Query apply(Function input, Context context) {
                assert input != null;
                assert input.arguments().size() == 1;
                BooleanQuery query = new BooleanQuery();

                query.add(process(input.arguments().get(0), context), BooleanClause.Occur.MUST_NOT);
                query.add(Queries.newMatchAllQuery(), BooleanClause.Occur.MUST);

                return query;
            }
        }

        static class IsNullQuery implements FunctionToQuery {

            @Override
            public Query apply(Function input, Context context) {
                assert input != null;
                assert input.arguments().size() == 1;
                Symbol arg = input.arguments().get(0);
                if (arg.symbolType() != SymbolType.REFERENCE) {
                    return null;
                }
                Reference reference = (Reference)arg;

                String columnName = reference.info().ident().columnIdent().fqn();
                QueryBuilderHelper builderHelper = QueryBuilderHelper.forType(reference.valueType());
                BooleanFilter boolFilter = new BooleanFilter();
                boolFilter.add(builderHelper.rangeFilter(columnName, null, null, true, true),
                        BooleanClause.Occur.MUST_NOT);
                return new FilteredQuery(Queries.newMatchAllQuery(), boolFilter);
            }
        }

        static class EqQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> tuple = super.prepare(input);
                if (tuple == null) {
                    return null;
                }
                Reference reference = tuple.v1();
                Literal literal = tuple.v2();
                String columnName = reference.info().ident().columnIdent().fqn();
                if (DataTypes.isCollectionType(reference.valueType()) && DataTypes.isCollectionType(literal.valueType())) {

                    // create boolean filter with term filters to pre-filter the result before applying the functionQuery.
                    BooleanFilter boolTermsFilter = new BooleanFilter();
                    DataType type = literal.valueType();
                    while (DataTypes.isCollectionType(type)) {
                        type = ((CollectionType) type).innerType();
                    }
                    QueryBuilderHelper builder = QueryBuilderHelper.forType(type);
                    Object value = literal.value();
                    buildTermsQuery(boolTermsFilter, value, columnName, builder);

                    if (boolTermsFilter.clauses().isEmpty()) {
                        // all values are null...
                        return genericFunctionQuery(input, context.inputSymbolVisitor, context.searchContext);
                    }

                    // wrap boolTermsFilter and genericFunction filter in an additional BooleanFilter to control the ordering of the filters
                    // termsFilter is applied first
                    // afterwards the more expensive genericFunctionFilter
                    BooleanFilter filterClauses = new BooleanFilter();
                    filterClauses.add(boolTermsFilter, BooleanClause.Occur.MUST);
                    filterClauses.add(
                            genericFunctionFilter(input, context.inputSymbolVisitor, context.searchContext),
                            BooleanClause.Occur.MUST);
                    return new FilteredQuery(Queries.newMatchAllQuery(), filterClauses);
                }
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.eq(columnName, tuple.v2().value());
            }

            private void buildTermsQuery(BooleanFilter booleanFilter,
                                            Object value,
                                            String columnName,
                                            QueryBuilderHelper builder) {
                if (value == null) {
                    return;
                }
                if (value.getClass().isArray()) {
                    Object[] array = (Object[]) value;
                    for (Object o : array) {
                        buildTermsQuery(booleanFilter, o, columnName, builder);
                    }
                } else {
                    booleanFilter.add(builder.eqFilter(columnName, value), BooleanClause.Occur.MUST);
                }
            }
        }

        class AndQuery implements FunctionToQuery {
            @Override
            public Query apply(Function input, Context context) {
                assert input != null;
                BooleanQuery query = new BooleanQuery();
                for (Symbol symbol : input.arguments()) {
                    query.add(process(symbol, context), BooleanClause.Occur.MUST);
                }
                return query;
            }
        }

        class OrQuery implements FunctionToQuery {
            @Override
            public Query apply(Function input, Context context) {
                assert input != null;
                BooleanQuery query = new BooleanQuery();
                query.setMinimumNumberShouldMatch(1);
                for (Symbol symbol : input.arguments()) {
                    query.add(process(symbol, context), BooleanClause.Occur.SHOULD);
                }
                return query;
            }
        }

        static class AnyRangeQuery extends AbstractAnyQuery {

            private final RangeQuery rangeQuery;
            private final RangeQuery inverseRangeQuery;

            AnyRangeQuery(String comparison, String inverseComparison) {
                rangeQuery = new RangeQuery(comparison);
                inverseRangeQuery = new RangeQuery(inverseComparison);
            }

            @Override
            protected Query applyArrayReference(Reference arrayReference, Literal literal, Context context) throws IOException {
                // 1 < ANY (array_col) --> array_col > 1
                return rangeQuery.toQuery(
                        arrayReference,
                        ((CollectionType)arrayReference.valueType()).innerType(),
                        literal.value()
                );
            }

            @Override
            protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) throws IOException {
                // col < ANY ([1,2,3]) --> or(col<1, col<2, col<3)
                BooleanQuery booleanQuery = new BooleanQuery();
                booleanQuery.setMinimumNumberShouldMatch(1);
                for (Object value : toIterable(arrayLiteral.value())) {
                    booleanQuery.add(inverseRangeQuery.toQuery(reference, reference.valueType(), value), BooleanClause.Occur.SHOULD);
                }
                return booleanQuery;
            }
        }

        static class RangeQuery extends CmpQuery {

            private final boolean includeLower;
            private final boolean includeUpper;
            private final com.google.common.base.Function<Object, Tuple<?, ?>> boundsFunction;

            private static final com.google.common.base.Function<Object, Tuple<?, ?>> LOWER_BOUND = new com.google.common.base.Function<Object, Tuple<?,?>>() {
                @javax.annotation.Nullable
                @Override
                public Tuple<?, ?> apply(@Nullable Object input) {
                    return new Tuple<>(input, null);
                }
            };

            private static final com.google.common.base.Function<Object, Tuple<?, ?>> UPPER_BOUND = new com.google.common.base.Function<Object, Tuple<?,?>>() {
                @Override
                public Tuple<?, ?> apply(Object input) {
                    return new Tuple<>(null, input);
                }
            };

            public RangeQuery(String comparison) {
                switch (comparison) {
                    case "lt":
                        boundsFunction = UPPER_BOUND;
                        includeLower = false;
                        includeUpper = false;
                        break;
                    case "gt":
                        boundsFunction = LOWER_BOUND;
                        includeLower = false;
                        includeUpper = false;
                        break;
                    case "lte":
                        boundsFunction = UPPER_BOUND;
                        includeLower = false;
                        includeUpper = true;
                        break;
                    case "gte":
                        boundsFunction = LOWER_BOUND;
                        includeLower = true;
                        includeUpper = false;
                        break;
                    default:
                        throw new IllegalArgumentException("invalid comparison");
                }
            }

            @Override
            public Query apply(Function input, Context context) throws IOException {
                Tuple<Reference, Literal> tuple = super.prepare(input);
                if (tuple == null) {
                    return null;
                }
                return toQuery(tuple.v1(), tuple.v1().valueType(), tuple.v2().value());
            }

            public Query toQuery(Reference reference, DataType type, Object value) {
                String columnName = reference.info().ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(type);
                Tuple<?, ?> bounds = boundsFunction.apply(value);
                assert bounds != null;
                return builder.rangeQuery(columnName, bounds.v1(), bounds.v2(), includeLower, includeUpper);
            }
        }

        static class ToMatchQuery implements FunctionToQuery {

            @Override
            public Query apply(Function input, Context context) throws IOException {
                List<Symbol> arguments = input.arguments();
                assert arguments.size() == 4 : "invalid number of arguments";
                assert Symbol.isLiteral(arguments.get(0), DataTypes.OBJECT);
                assert Symbol.isLiteral(arguments.get(1), DataTypes.STRING);
                assert Symbol.isLiteral(arguments.get(2), DataTypes.STRING);
                assert Symbol.isLiteral(arguments.get(3), DataTypes.OBJECT);

                @SuppressWarnings("unchecked")
                Map<String, Object> fields = (Map) ((Literal) arguments.get(0)).value();
                BytesRef queryString = (BytesRef) ((Literal) arguments.get(1)).value();
                BytesRef matchType = (BytesRef) ((Literal) arguments.get(2)).value();
                Map options = (Map) ((Literal) arguments.get(3)).value();

                checkArgument(queryString != null, "cannot use NULL as query term in match predicate");

                MatchQueryBuilder queryBuilder;
                if (fields.size() == 1) {
                    queryBuilder = new MatchQueryBuilder(context.searchContext, context.indexCache, matchType, options);
                } else {
                    queryBuilder = new MultiMatchQueryBuilder(context.searchContext, context.indexCache, matchType, options);
                }
                return queryBuilder.query(fields, queryString);
            }
        }

        static class RegexpMatchQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) throws IOException {
                Tuple<Reference, Literal> prepare = prepare(input);
                if (prepare == null) { return null; }
                String fieldName = prepare.v1().info().ident().columnIdent().fqn();
                Object value = prepare.v2().value();

                // FIXME: nobody knows how Strings can arrive here
                if (value instanceof String) {
                    if (isPcrePattern(value)) {
                        return new RegexQuery(new Term(fieldName, (String) value));
                    } else {
                        return new RegexpQuery(new Term(fieldName, (String) value), RegExp.ALL);
                    }
                }

                if (value instanceof BytesRef) {
                    if (isPcrePattern(value)) {
                        return new RegexQuery(new Term(fieldName, (BytesRef) value));
                    } else {
                        return new RegexpQuery(new Term(fieldName, (BytesRef) value), RegExp.ALL);
                    }
                }

                throw new IllegalArgumentException("Can only use ~ with patterns of type string");
            }
        }

        static class RegexMatchQueryCaseInsensitive extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) throws IOException {
                Tuple<Reference, Literal> prepare = prepare(input);
                if (prepare == null) { return null; }
                String fieldName = prepare.v1().info().ident().columnIdent().fqn();
                Object value = prepare.v2().value();

                // FIXME: nobody knows how Strings can arrive here
                if (value instanceof String) {
                    RegexQuery query = new RegexQuery(new Term(fieldName, (String) value));
                    query.setRegexImplementation(new JavaUtilRegexCapabilities(
                            JavaUtilRegexCapabilities.FLAG_CASE_INSENSITIVE |
                            JavaUtilRegexCapabilities.FLAG_UNICODE_CASE));
                    return query;
                }

                if (value instanceof BytesRef) {
                    RegexQuery query = new RegexQuery(new Term(fieldName, (BytesRef) value));
                    query.setRegexImplementation(new JavaUtilRegexCapabilities(
                            JavaUtilRegexCapabilities.FLAG_CASE_INSENSITIVE |
                            JavaUtilRegexCapabilities.FLAG_UNICODE_CASE));
                    return query;
                }

                throw new IllegalArgumentException("Can only use ~* with patterns of type string");
            }
        }

        /**
         * interface for functions that can be used to generate a query from inner functions.
         * Has only a single method {@link #apply(io.crate.planner.symbol.Function, io.crate.planner.symbol.Function, io.crate.lucene.LuceneQueryBuilder.Context)}
         *
         * e.g. in a query like
         * <pre>
         *     where distance(p1, 'POINT (10 20)') = 20
         * </pre>
         *
         * The first parameter (parent) would be the "eq" function.
         * The second parameter (inner) would be the "distance" function.
         *
         * The returned Query must "contain" both the parent and inner functions.
         */
         interface InnerFunctionToQuery {

            /**
             * returns a query for the given functions or null if it can't build a query.
             */
            @Nullable
            Query apply(Function parent, Function inner, Context context) throws IOException;
        }

        /**
         * for where within(shape1, shape2) = [ true | false ]
         */
        static class WithinQuery implements FunctionToQuery, InnerFunctionToQuery {

            @Override
            public Query apply(Function parent, Function inner, Context context) throws IOException {
                FunctionLiteralPair outerPair = new FunctionLiteralPair(parent);
                if (!outerPair.isValid()) {
                    return null;
                }
                Query query = getQuery(inner, context);
                if (query == null) return null;
                Boolean negate = !(Boolean) outerPair.input().value();
                if (negate) {
                    BooleanQuery booleanQuery = new BooleanQuery();
                    booleanQuery.add(query, BooleanClause.Occur.MUST_NOT);
                    return booleanQuery;
                } else {
                    return query;
                }
            }

            private Query getQuery(Function inner, Context context) {
                RefLiteralPair innerPair = new RefLiteralPair(inner);
                if (!innerPair.isValid()) {
                    return null;
                }
                GeoPointFieldMapper mapper = getGeoPointFieldMapper(
                        innerPair.reference().info().ident().columnIdent().fqn(),
                        context.searchContext
                );
                Shape shape = (Shape) innerPair.input().value();
                Geometry geometry = JtsSpatialContext.GEO.getGeometryFrom(shape);
                IndexGeoPointFieldData fieldData = context.searchContext.fieldData().getForField(mapper);
                Filter filter;
                if (geometry.isRectangle()) {
                    Rectangle boundingBox = shape.getBoundingBox();
                    filter = new InMemoryGeoBoundingBoxFilter(
                            new GeoPoint(boundingBox.getMaxY(), boundingBox.getMinX()),
                            new GeoPoint(boundingBox.getMinY(), boundingBox.getMaxX()),
                            fieldData
                    );
                } else {
                    Coordinate[] coordinates = geometry.getCoordinates();
                    GeoPoint[] points = new GeoPoint[coordinates.length];
                    for (int i = 0; i < coordinates.length; i++) {
                        Coordinate coordinate = coordinates[i];
                        points[i] = new GeoPoint(coordinate.y, coordinate.x);
                    }
                    filter = new GeoPolygonFilter(fieldData, points);
                }
                return new FilteredQuery(Queries.newMatchAllQuery(), context.indexCache.filter().cache(filter));
            }

            @Override
            public Query apply(Function input, Context context) throws IOException {
                return getQuery(input, context);
            }
        }

        class DistanceQuery implements InnerFunctionToQuery {

            final GeoDistance geoDistance = GeoDistance.DEFAULT;
            final String optimizeBox = "memory";

            /**
             *
             * @param parent the outer function. E.g. in the case of
             *     <pre>where distance(p1, POINT (10 20)) > 20</pre>
             *     this would be
             *     <pre>gt( \<inner function\>,  20)</pre>
             * @param inner has to be the distance function
             */
            @Override
            public Query apply(Function parent, Function inner, Context context) {
                assert inner.info().ident().name().equals(DistanceFunction.NAME);

                RefLiteralPair distanceRefLiteral = new RefLiteralPair(inner);
                if (!distanceRefLiteral.isValid()) {
                    // can't use distance filter without literal, fallback to genericFunction
                    return null;
                }
                FunctionLiteralPair functionLiteralPair = new FunctionLiteralPair(parent);
                if (!functionLiteralPair.isValid()) {
                    // must be something like eq(distance(..), non-literal) - fallback to genericFunction
                    return null;
                }
                Double distance = DataTypes.DOUBLE.value(functionLiteralPair.input().value());

                String fieldName = distanceRefLiteral.reference().info().ident().columnIdent().fqn();
                FieldMapper mapper = getGeoPointFieldMapper(fieldName, context.searchContext);
                GeoPointFieldMapper geoMapper = ((GeoPointFieldMapper) mapper);
                IndexGeoPointFieldData fieldData = context.searchContext.fieldData().getForField(mapper);

                Input geoPointInput = distanceRefLiteral.input();
                Double[] pointValue = (Double[]) geoPointInput.value();
                double lat = pointValue[1];
                double lon = pointValue[0];

                String parentName = functionLiteralPair.functionName();

                Double from = null;
                Double to = null;
                boolean includeLower = false;
                boolean includeUpper = false;

                switch (parentName) {
                    case EqOperator.NAME:
                        includeLower = true;
                        includeUpper = true;
                        from = distance;
                        to = distance;
                        break;
                    case LteOperator.NAME:
                        includeUpper = true;
                        to = distance;
                        break;
                    case LtOperator.NAME:
                        to = distance;
                        break;
                    case GteOperator.NAME:
                        from = distance;
                        includeLower = true;
                        break;
                    case GtOperator.NAME:
                        from = distance;
                        break;
                    default:
                        // invalid operator? give up
                        return null;
                }
                GeoPoint geoPoint = new GeoPoint(lat, lon);
                Filter filter = new GeoDistanceRangeFilter(
                        geoPoint,
                        from,
                        to,
                        includeLower,
                        includeUpper,
                        geoDistance,
                        geoMapper,
                        fieldData,
                        optimizeBox
                );
                return new FilteredQuery(Queries.newMatchAllQuery(), context.indexCache.filter().cache(filter));
            }
        }

        private static GeoPointFieldMapper getGeoPointFieldMapper(String fieldName, SearchContext searchContext) {
            MapperService.SmartNameFieldMappers smartMappers = searchContext.smartFieldMappers(fieldName);
            if (smartMappers == null || !smartMappers.hasMapper()) {
                throw new IllegalArgumentException(String.format("column \"%s\" doesn't exist", fieldName));
            }
            FieldMapper mapper = smartMappers.mapper();
            if (!(mapper instanceof GeoPointFieldMapper)) {
                throw new IllegalArgumentException(String.format("column \"%s\" isn't of type geo_point", fieldName));
            }
            return (GeoPointFieldMapper) mapper;
        }

        private static final EqQuery eqQuery = new EqQuery();
        private static final RangeQuery ltQuery = new RangeQuery("lt");
        private static final RangeQuery lteQuery = new RangeQuery("lte");
        private static final RangeQuery gtQuery = new RangeQuery("gt");
        private static final RangeQuery gteQuery = new RangeQuery("gte");
        private static final WithinQuery withinQuery = new WithinQuery();
        private final ImmutableMap<String, FunctionToQuery> functions =
                ImmutableMap.<String, FunctionToQuery>builder()
                        .put(WithinFunction.NAME, withinQuery)
                        .put(AndOperator.NAME, new AndQuery())
                        .put(OrOperator.NAME, new OrQuery())
                        .put(EqOperator.NAME, eqQuery)
                        .put(LtOperator.NAME, ltQuery)
                        .put(LteOperator.NAME, lteQuery)
                        .put(GteOperator.NAME, gteQuery)
                        .put(GtOperator.NAME, gtQuery)
                        .put(LikeOperator.NAME, new LikeQuery())
                        .put(InOperator.NAME, new InQuery())
                        .put(NotPredicate.NAME, new NotQuery())
                        .put(IsNullPredicate.NAME, new IsNullQuery())
                        .put(MatchPredicate.NAME, new ToMatchQuery())
                        .put(AnyEqOperator.NAME, new AnyEqQuery())
                        .put(AnyNeqOperator.NAME, new AnyNeqQuery())
                        .put(AnyLtOperator.NAME, new AnyRangeQuery("gt", "lt"))
                        .put(AnyLteOperator.NAME, new AnyRangeQuery("gte", "lte"))
                        .put(AnyGteOperator.NAME, new AnyRangeQuery("lte", "gte"))
                        .put(AnyGtOperator.NAME, new AnyRangeQuery("lt", "gt"))
                        .put(AnyLikeOperator.NAME, new AnyLikeQuery())
                        .put(AnyNotLikeOperator.NAME, new AnyNotLikeQuery())
                        .put(RegexpMatchOperator.NAME, new RegexpMatchQuery())
                        .put(RegexpMatchCaseInsensitiveOperator.NAME, new RegexMatchQueryCaseInsensitive())
                        .build();

        private final ImmutableMap<String, InnerFunctionToQuery> innerFunctions =
                ImmutableMap.<String, InnerFunctionToQuery>builder()
                        .put(DistanceFunction.NAME, new DistanceQuery())
                        .put(WithinFunction.NAME, withinQuery)
                        .build();

        @Override
        public Query visitFunction(Function function, Context context) {
            assert function != null;
            if (fieldIgnored(function, context)) {
                return Queries.newMatchAllQuery();
            }
            validateNoUnsupportedFields(function, context);

            FunctionToQuery toQuery = functions.get(function.info().ident().name());
            if (toQuery == null) {
                return genericFunctionQuery(function, context.inputSymbolVisitor, context.searchContext);
            }

            Query query;
            try {
                query = toQuery.apply(function, context);
            } catch (IOException e) {
                throw ExceptionsHelper.convertToRuntime(e);
            } catch (UnsupportedOperationException e) {
                return genericFunctionQuery(function, context.inputSymbolVisitor, context.searchContext);
            }
            if (query == null) {
                query = queryFromInnerFunction(function, context);
                if (query == null) {
                    return genericFunctionQuery(function, context.inputSymbolVisitor, context.searchContext);
                }
            }
            return query;
        }

        private Query queryFromInnerFunction(Function function, Context context) {
            for (Symbol symbol : function.arguments()) {
                if (symbol.symbolType() == SymbolType.FUNCTION) {
                    String functionName = ((Function) symbol).info().ident().name();
                    InnerFunctionToQuery functionToQuery = innerFunctions.get(functionName);
                    if (functionToQuery != null) {
                        try {
                            Query query = functionToQuery.apply(function, (Function)symbol, context);
                            if (query != null) {
                                return query;
                            }
                        } catch (IOException e) {
                            throw ExceptionsHelper.convertToRuntime(e);
                        }
                    }
                }
            }
            return null;
        }

        private boolean fieldIgnored(Function function, Context context) {
            if (function.arguments().size() != 2) {
                return false;
            }

            Symbol left = function.arguments().get(0);
            Symbol right = function.arguments().get(1);
            if (left.symbolType() == SymbolType.REFERENCE && right.symbolType().isValueSymbol()) {
                String columnName = ((Reference) left).info().ident().columnIdent().name();
                if (Context.FILTERED_FIELDS.contains(columnName)) {
                    context.filteredFieldValues.put(columnName, ((Input) right).value());
                    return true;
                }
                String unsupportedMessage = Context.UNSUPPORTED_FIELDS.get(columnName);
                if (unsupportedMessage != null) {
                    throw new UnsupportedFeatureException(unsupportedMessage);
                }
            }
            return false;
        }

        @Nullable
        private String validateNoUnsupportedFields(Function function, Context context){
            if(function.arguments().size() != 2){
                return null;
            }
            Symbol left = function.arguments().get(0);
            Symbol right = function.arguments().get(1);
            if (left.symbolType() == SymbolType.REFERENCE && right.symbolType().isValueSymbol()) {
                String columnName = ((Reference) left).info().ident().columnIdent().name();
                String unsupportedMessage = context.unsupportedMessage(columnName);
                if(unsupportedMessage != null){
                    throw new UnsupportedFeatureException(unsupportedMessage);
                }
            }
            return null;
        }

        private static Filter genericFunctionFilter(Function function,
                                                    CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor,
                                                    SearchContext searchContext) {
            if (function.valueType() != DataTypes.BOOLEAN) {
                raiseUnsupported(function);
            }
            // avoid field-cache
            // reason1: analyzed columns or columns with index off wouldn't work
            //   substr(n, 1, 1) in the case of n => analyzed would throw an error because n would be an array
            // reason2: would have to load each value into the field cache
            function = (Function)DocReferenceConverter.convertIf(function, Predicates.<Reference>alwaysTrue());

            final CollectInputSymbolVisitor.Context ctx = inputSymbolVisitor.extractImplementations(function);
            assert ctx.topLevelInputs().size() == 1;
            @SuppressWarnings("unchecked")
            final Input<Boolean> condition = (Input<Boolean>) ctx.topLevelInputs().get(0);
            @SuppressWarnings("unchecked")
            final List<LuceneCollectorExpression> expressions = ctx.docLevelExpressions();
            final CollectorContext collectorContext = new CollectorContext();
            collectorContext.searchContext(searchContext);
            collectorContext.visitor(new LuceneDocCollector.CollectorFieldsVisitor(expressions.size()));

            for (LuceneCollectorExpression expression : expressions) {
                expression.startCollect(collectorContext);
            }
            return new Filter() {
                @Override
                public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
                    for (LuceneCollectorExpression expression : expressions) {
                        expression.setNextReader(context.reader().getContext());
                    }
                    return BitsFilteredDocIdSet.wrap(
                            new FunctionDocSet(
                                    context.reader(),
                                    collectorContext.visitor(),
                                    condition,
                                    expressions,
                                    context.reader().maxDoc(),
                                    acceptDocs
                            ),
                            acceptDocs
                    );
                }
            };
        }

        private static Query genericFunctionQuery(Function function,
                                                  CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor,
                                                  SearchContext searchContext) {
            return new FilteredQuery(
                    Queries.newMatchAllQuery(),
                    genericFunctionFilter(function, inputSymbolVisitor, searchContext));
        }

        static class FunctionDocSet extends MatchDocIdSet {

            private final AtomicReader reader;
            private final LuceneDocCollector.CollectorFieldsVisitor fieldsVisitor;
            private final Input<Boolean> condition;
            private final List<LuceneCollectorExpression> expressions;
            private final boolean fieldsVisitorEnabled;

            protected FunctionDocSet(AtomicReader reader,
                                     @Nullable LuceneDocCollector.CollectorFieldsVisitor fieldsVisitor,
                                     Input<Boolean> condition,
                                     List<LuceneCollectorExpression> expressions,
                                     int maxDoc,
                                     @Nullable Bits acceptDocs) {
                super(maxDoc, acceptDocs);
                this.reader = reader;
                this.fieldsVisitor = fieldsVisitor;
                //noinspection SimplifiableConditionalExpression
                this.fieldsVisitorEnabled = fieldsVisitor == null ? false : fieldsVisitor.required();
                this.condition = condition;
                this.expressions = expressions;
            }

            @Override
            protected boolean matchDoc(int doc) {
                if (fieldsVisitorEnabled) {
                    fieldsVisitor.reset();
                    try {
                        reader.document(doc, fieldsVisitor);
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }
                for (LuceneCollectorExpression expression : expressions) {
                    expression.setNextDocId(doc);
                }
                Boolean value = condition.value();
                if (value == null) {
                    return false;
                }
                return value;
            }
        }

        private static Query raiseUnsupported(Function function) {
            throw new UnsupportedOperationException(
                    SymbolFormatter.format("Cannot convert function %s into a query", function));
        }

        @Override
        public Query visitReference(Reference symbol, Context context) {
            // called for queries like: where boolColumn
            if (symbol.valueType() == DataTypes.BOOLEAN) {
                return QueryBuilderHelper.forType(DataTypes.BOOLEAN).eq(symbol.info().ident().columnIdent().fqn(), true);
            }
            return super.visitReference(symbol, context);
        }

        @Override
        protected Query visitSymbol(Symbol symbol, Context context) {
            throw new UnsupportedOperationException(
                    SymbolFormatter.format("Can't build query from symbol %s", symbol));
        }
    }

    static class FunctionLiteralPair {

        private final String functionName;
        private final Function function;
        private final Input input;

        FunctionLiteralPair(Function outerFunction) {
            assert outerFunction.arguments().size() == 2 : "function requires 2 arguments";
            Symbol left = outerFunction.arguments().get(0);
            Symbol right = outerFunction.arguments().get(1);

            functionName = outerFunction.info().ident().name();

            if (left instanceof Function) {
                function = (Function) left;
            } else if (right instanceof Function) {
                function = (Function) right;
            } else {
                function = null;
            }

            if (left.symbolType().isValueSymbol()) {
                input = (Input) left;
            } else if (right.symbolType().isValueSymbol()) {
                input = (Input) right;
            } else {
                input = null;
            }
        }

        public boolean isValid() {
            return input != null && function != null;
        }

        public Input input() {
            return input;
        }

        public String functionName() {
            return functionName;
        }
    }

    static class RefLiteralPair {

        private final Reference reference;
        private final Input input;

        RefLiteralPair(Function function) {
            assert function.arguments().size() == 2 : "function requires 2 arguments";
            Symbol left = function.arguments().get(0);
            Symbol right = function.arguments().get(1);

            if (left instanceof Reference) {
                reference = (Reference) left;
            } else if (right instanceof Reference) {
                reference = (Reference) right;
            } else {
                reference = null;
            }

            if (left.symbolType().isValueSymbol()) {
                input = (Input) left;
            } else if (right.symbolType().isValueSymbol()) {
                input = (Input) right;
            } else {
                input = null;
            }
        }

        public boolean isValid() {
            return input != null && reference != null;
        }

        public Reference reference() {
            return reference;
        }

        public Input input() {
            return input;
        }
    }

    @SuppressWarnings("unchecked")
    private static BytesRef[] getBytesRefs(Object[] values, TermBuilder termBuilder) {
        BytesRef[] terms = new BytesRef[values.length];
        int i = 0;
        for (Object value : values) {
            terms[i] = termBuilder.term(value);
            i++;
        }
        return terms;
    }

    @SuppressWarnings("unchecked")
    private static BytesRef[] getBytesRefs(Collection values, TermBuilder termBuilder) {
        BytesRef[] terms = new BytesRef[values.size()];
        int i = 0;
        for (Object value : values) {
            terms[i] = termBuilder.term(value);
            i++;
        }
        return terms;
    }
}
