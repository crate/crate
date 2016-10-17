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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateArrays;
import com.vividsolutions.jts.geom.Geometry;
import io.crate.Constants;
import io.crate.analyze.MatchOptionsAnalysis;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.*;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.geo.GeoJSONUtils;
import io.crate.lucene.match.MatchQueryBuilder;
import io.crate.lucene.match.MultiMatchQueryBuilder;
import io.crate.metadata.DocReferenceConverter;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.collect.collectors.CollectorFieldsVisitor;
import io.crate.operation.operator.*;
import io.crate.operation.operator.any.*;
import io.crate.operation.predicate.IsNullPredicate;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.operation.predicate.NotPredicate;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.operation.scalar.geo.DistanceFunction;
import io.crate.operation.scalar.geo.WithinFunction;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.sandbox.queries.regex.JavaUtilRegexCapabilities;
import org.apache.lucene.sandbox.queries.regex.RegexQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.geopoint.search.GeoPointDistanceRangeQuery;
import org.apache.lucene.spatial.geopoint.search.GeoPointInPolygonQuery;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.util.GeoDistanceUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;
import org.elasticsearch.index.query.RegexpFlag;
import org.elasticsearch.index.search.geo.GeoDistanceRangeQuery;
import org.elasticsearch.index.search.geo.GeoPolygonQuery;
import org.elasticsearch.index.search.geo.InMemoryGeoBoundingBoxQuery;

import java.io.IOException;
import java.util.*;

import static io.crate.operation.scalar.regex.RegexMatcher.isPcrePattern;
import static org.apache.lucene.spatial.util.GeoEncodingUtils.TOLERANCE;

@Singleton
public class LuceneQueryBuilder {

    private static final ESLogger LOGGER = Loggers.getLogger(LuceneQueryBuilder.class);
    private final static Visitor VISITOR = new Visitor();
    private final CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor;

    @Inject
    public LuceneQueryBuilder(Functions functions) {
        inputSymbolVisitor = new CollectInputSymbolVisitor<>(functions, new LuceneReferenceResolver(null));
    }

    public Context convert(WhereClause whereClause,
                           MapperService mapperService,
                           IndexFieldDataService indexFieldDataService,
                           IndexCache indexCache) throws UnsupportedFeatureException {
        Context ctx = new Context(inputSymbolVisitor, mapperService, indexFieldDataService, indexCache);
        if (whereClause.noMatch()) {
            ctx.query = Queries.newMatchNoDocsQuery();
        } else if (!whereClause.hasQuery()) {
            ctx.query = Queries.newMatchAllQuery();
        } else {
            ctx.query = VISITOR.process(whereClause.query(), ctx);
        }
        if (LOGGER.isTraceEnabled()) {
            if (whereClause.hasQuery()) {
                LOGGER.trace("WHERE CLAUSE [{}] -> LUCENE QUERY [{}] ", SymbolPrinter.INSTANCE.printSimple(whereClause.query()), ctx.query);
            }
        }
        return ctx;
    }

    private static Query termsQuery(String columnName, Literal arrayLiteral) {
        List<Term> terms = getTerms(columnName, arrayLiteral);
        if (terms.isEmpty()) {
            return new MatchNoDocsQuery();
        }
        return new TermsQuery(terms);
    }

    private static List<Term> getTerms(String columnName, Literal arrayLiteral) {
        Object values = arrayLiteral.value();
        Collection valueCollection;
        if (values instanceof Collection) {
            valueCollection = (Collection) values;
        } else {
            valueCollection = Arrays.asList((Object[]) values);
        }
        return asTerms(columnName, valueCollection, TermBuilder.forType(arrayLiteral.valueType()));
    }

    private static List<Term> asTerms(String columnName, Collection values, TermBuilder termBuilder) {
        List<Term> terms = new ArrayList<>(values.size());
        for (Object value : values) {
            if (value == null) {
                // skipping null values is okay because TermsFilter is only used for ANY
                // and  x = ANY ([null]) shouldn't match even if X is null
                continue;
            }
            terms.add(new Term(columnName, termBuilder.term(value)));
        }
        return terms;
    }


    public static class Context {
        Query query;

        final Map<String, Object> filteredFieldValues = new HashMap<>();

        final CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor;
        final MapperService mapperService;
        final IndexFieldDataService fieldDataService;
        final IndexCache indexCache;

        Context(CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor,
                MapperService mapperService,
                IndexFieldDataService fieldDataService,
                IndexCache indexCache) {
            this.inputSymbolVisitor = inputSymbolVisitor;
            this.mapperService = mapperService;
            this.fieldDataService = fieldDataService;
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
        public String unsupportedMessage(String field) {
            return UNSUPPORTED_FIELDS.get(field);
        }

        /**
         * These fields are ignored in the whereClause.
         * If a filtered field is encountered the value of the literal is written into filteredFieldValues
         * (only applies to Function with 2 arguments and if left == reference and right == literal)
         */
        final static Set<String> FILTERED_FIELDS = new HashSet<String>() {{
            add("_score");
        }};

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

    public static String convertSqlLikeToLuceneWildcard(String wildcardString) {
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
            Query apply(Function input, Context context) throws IOException;
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
                return new Tuple<>((Reference) left, (Literal) right);
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
                    if (collectionSymbol instanceof Reference) {
                        return applyArrayReference((Reference) collectionSymbol, (Literal) left, context);
                    } else {
                        // no reference found (maybe subscript) in ANY expression -> fallback to slow generic function filter
                        return null;
                    }
                } else if (left instanceof Reference && collectionSymbol.symbolType().isValueSymbol()) {
                    return applyArrayLiteral((Reference) left, (Literal) collectionSymbol, context);
                } else {
                    // might be the case if the left side is a function -> will fallback to (slow) generic function filter
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
                            input = new BytesRef((String) input);
                        }
                        return input;
                    }
                });
            }

            protected abstract Query applyArrayReference(Reference arrayReference, Literal literal, Context context) throws IOException;

            protected abstract Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) throws IOException;
        }

        static class AnyEqQuery extends AbstractAnyQuery {

            @Override
            protected Query applyArrayReference(Reference arrayReference, Literal literal, Context context) throws IOException {
                QueryBuilderHelper builder = QueryBuilderHelper.forType(((CollectionType) arrayReference.valueType()).innerType());
                return builder.eq(arrayReference.ident().columnIdent().fqn(), literal.value());
            }

            @Override
            protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) throws IOException {
                String columnName = reference.ident().columnIdent().fqn();
                return termsQuery(columnName, arrayLiteral);
            }
        }

        static class AnyNeqQuery extends AbstractAnyQuery {

            @Override
            protected Query applyArrayReference(Reference arrayReference, Literal literal, Context context) throws IOException {
                // 1 != any ( col ) -->  gt 1 or lt 1
                String columnName = arrayReference.ident().columnIdent().fqn();
                Object value = literal.value();

                QueryBuilderHelper builder = QueryBuilderHelper.forType(arrayReference.valueType());
                BooleanQuery.Builder query = new BooleanQuery.Builder();
                query.setMinimumNumberShouldMatch(1);
                query.add(
                    builder.rangeQuery(columnName, value, null, false, false),
                    BooleanClause.Occur.SHOULD
                );
                query.add(
                    builder.rangeQuery(columnName, null, value, false, false),
                    BooleanClause.Occur.SHOULD
                );
                return query.build();
            }

            @Override
            protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) throws IOException {
                //  col != ANY ([1,2,3]) --> not(col=1 and col=2 and col=3)
                String columnName = reference.ident().columnIdent().fqn();
                QueryBuilderHelper helper = QueryBuilderHelper.forType(reference.valueType());

                BooleanQuery.Builder andBuilder = new BooleanQuery.Builder();
                for (Object value : toIterable(arrayLiteral.value())) {
                    andBuilder.add(helper.eq(columnName, value), BooleanClause.Occur.MUST);
                }
                return Queries.not(andBuilder.build());
            }
        }

        static class AnyNotLikeQuery extends AbstractAnyQuery {

            @Override
            protected Query applyArrayReference(Reference arrayReference, Literal literal, Context context) throws IOException {
                String regexString = LikeOperator.patternToRegex(BytesRefs.toString(literal.value()), LikeOperator.DEFAULT_ESCAPE, false);
                regexString = regexString.substring(1, regexString.length() - 1);
                String notLike = negateWildcard(regexString);

                return new RegexpQuery(new Term(
                    arrayReference.ident().columnIdent().fqn(),
                    notLike),
                    RegexpFlag.COMPLEMENT.value()
                );
            }

            @Override
            protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) throws IOException {
                // col not like ANY (['a', 'b']) --> not(and(like(col, 'a'), like(col, 'b')))
                String columnName = reference.ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(reference.valueType());

                BooleanQuery.Builder andLikeQueries = new BooleanQuery.Builder();
                for (Object value : toIterable(arrayLiteral.value())) {
                    andLikeQueries.add(builder.like(columnName, value, context.indexCache.query()), BooleanClause.Occur.MUST);
                }
                return Queries.not(andLikeQueries.build());
            }
        }

        static class AnyLikeQuery extends AbstractAnyQuery {

            private static final LikeQuery LIKE_QUERY = new LikeQuery();

            @Override
            protected Query applyArrayReference(Reference arrayReference, Literal literal, Context context) throws IOException {
                return LIKE_QUERY.toQuery(arrayReference, literal.value(), context);
            }

            @Override
            protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) throws IOException {
                // col like ANY (['a', 'b']) --> or(like(col, 'a'), like(col, 'b'))
                BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
                booleanQuery.setMinimumNumberShouldMatch(1);
                for (Object value : toIterable(arrayLiteral.value())) {
                    booleanQuery.add(LIKE_QUERY.toQuery(reference, value, context), BooleanClause.Occur.SHOULD);
                }
                return booleanQuery.build();
            }
        }

        static class LikeQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> tuple = prepare(input);
                if (tuple == null) {
                    return null;
                }
                return toQuery(tuple.v1(), tuple.v2().value(), context);
            }

            public Query toQuery(Reference reference, Object value, Context context) {

                String columnName = reference.ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(reference.valueType());
                return builder.like(columnName, value, context.indexCache.query());
            }
        }

        static class InQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) throws IOException {
                Tuple<Reference, Literal> tuple = prepare(input);
                if (tuple == null) {
                    return null;
                }
                String field = tuple.v1().ident().columnIdent().fqn();
                Literal literal = tuple.v2();
                return termsQuery(field, literal);
            }
        }

        class NotQuery implements FunctionToQuery {

            private class SymbolToNotNullContext {
                private final HashSet<Reference> references = new HashSet<>();
                boolean containsNullCanMatchFunction = false;

                boolean add(Reference symbol) {
                    return references.add(symbol);
                }

                Set<Reference> references() {
                    return references;
                }
            }

            private class SymbolToNotNullRangeQueryArgs extends SymbolVisitor<SymbolToNotNullContext, Void> {

                private final Set<String> NULL_CAN_MATCH_FUNCTIONS = ImmutableSet.of("any", "coalesce");

                private boolean nullCanMatch(Function symbol) {
                    return NULL_CAN_MATCH_FUNCTIONS.contains(symbol.info().ident().name());
                }

                @Override
                public Void visitReference(Reference symbol, SymbolToNotNullContext context) {
                    context.add(symbol);
                    return null;
                }

                @Override
                public Void visitFunction(Function symbol, SymbolToNotNullContext context) {
                    if (!nullCanMatch(symbol)) {
                        for (Symbol arg: symbol.arguments()) {
                            process(arg, context);
                        }
                    } else {
                        context.containsNullCanMatchFunction = true;
                    }
                    return null;
                }
            }

            private final SymbolToNotNullRangeQueryArgs INNER_VISITOR = new SymbolToNotNullRangeQueryArgs();

            @Override
            public Query apply(Function input, Context context) {
                assert input != null;
                assert input.arguments().size() == 1;
                /**
                 * not null -> null     -> no match
                 * not true -> false    -> no match
                 * not false -> true    -> match
                 */

                // handles not true / not false
                Symbol arg = input.arguments().get(0);
                Query innerQuery = process(arg, context);
                Query notX = Queries.not(innerQuery);

                // not x =  not x & x is not null
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                builder.add(notX, BooleanClause.Occur.MUST);

                SymbolToNotNullContext ctx = new SymbolToNotNullContext();
                INNER_VISITOR.process(arg, ctx);
                for (Reference reference : ctx.references()) {
                    String columnName = reference.ident().columnIdent().fqn();
                    QueryBuilderHelper builderHelper = QueryBuilderHelper.forType(reference.valueType());
                    builder.add(builderHelper.rangeQuery(columnName, null, null, true, true), BooleanClause.Occur.MUST);
                }
                if (ctx.containsNullCanMatchFunction) {
                    FunctionInfo isNullInfo = IsNullPredicate.generateInfo(Collections.singletonList(arg.valueType()));
                    Function isNullFunction = new Function(isNullInfo, Collections.singletonList(arg));
                    builder.add(
                        Queries.not(genericFunctionFilter(isNullFunction, context)),
                        BooleanClause.Occur.MUST
                    );
                }
                return builder.build();
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
                Reference reference = (Reference) arg;

                String columnName = reference.ident().columnIdent().fqn();
                QueryBuilderHelper builderHelper = QueryBuilderHelper.forType(reference.valueType());
                return Queries.not(builderHelper.rangeQuery(columnName, null, null, true, true));
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
                String columnName = reference.ident().columnIdent().fqn();
                if (DataTypes.isCollectionType(reference.valueType()) &&
                    DataTypes.isCollectionType(literal.valueType())) {
                    List<Term> terms = getTerms(columnName, literal);
                    if (terms.isEmpty()) {
                        return genericFunctionFilter(input, context);
                    }
                    Query termsQuery = new TermsQuery(terms);

                    // wrap boolTermsFilter and genericFunction filter in an additional BooleanFilter to control the ordering of the filters
                    // termsFilter is applied first
                    // afterwards the more expensive genericFunctionFilter
                    BooleanQuery.Builder filterClauses = new BooleanQuery.Builder();
                    filterClauses.add(termsQuery, BooleanClause.Occur.MUST);
                    filterClauses.add(genericFunctionFilter(input, context), BooleanClause.Occur.MUST);
                    return filterClauses.build();
                }
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.eq(columnName, tuple.v2().value());
            }
        }

        class AndQuery implements FunctionToQuery {
            @Override
            public Query apply(Function input, Context context) {
                assert input != null;
                BooleanQuery.Builder query = new BooleanQuery.Builder();
                for (Symbol symbol : input.arguments()) {
                    query.add(process(symbol, context), BooleanClause.Occur.MUST);
                }
                return query.build();
            }
        }

        class OrQuery implements FunctionToQuery {
            @Override
            public Query apply(Function input, Context context) {
                assert input != null;
                BooleanQuery.Builder query = new BooleanQuery.Builder();
                query.setMinimumNumberShouldMatch(1);
                for (Symbol symbol : input.arguments()) {
                    query.add(process(symbol, context), BooleanClause.Occur.SHOULD);
                }
                return query.build();
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
                    ((CollectionType) arrayReference.valueType()).innerType(),
                    literal.value()
                );
            }

            @Override
            protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) throws IOException {
                // col < ANY ([1,2,3]) --> or(col<1, col<2, col<3)
                BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
                booleanQuery.setMinimumNumberShouldMatch(1);
                for (Object value : toIterable(arrayLiteral.value())) {
                    booleanQuery.add(inverseRangeQuery.toQuery(reference, reference.valueType(), value), BooleanClause.Occur.SHOULD);
                }
                return booleanQuery.build();
            }
        }

        static class RangeQuery extends CmpQuery {

            private final boolean includeLower;
            private final boolean includeUpper;
            private final com.google.common.base.Function<Object, Tuple<?, ?>> boundsFunction;

            private static final com.google.common.base.Function<Object, Tuple<?, ?>> LOWER_BOUND = new com.google.common.base.Function<Object, Tuple<?, ?>>() {
                @javax.annotation.Nullable
                @Override
                public Tuple<?, ?> apply(@Nullable Object input) {
                    return new Tuple<>(input, null);
                }
            };

            private static final com.google.common.base.Function<Object, Tuple<?, ?>> UPPER_BOUND = new com.google.common.base.Function<Object, Tuple<?, ?>>() {
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
                String columnName = reference.ident().columnIdent().fqn();
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
                assert Symbol.isLiteral(arguments.get(0), DataTypes.OBJECT); // fields
                assert Symbol.isLiteral(arguments.get(2), DataTypes.STRING); // matchType

                Symbol queryTerm = arguments.get(1);
                Preconditions.checkArgument(queryTerm instanceof Input, "queryTerm must be a literal");
                Object queryTermVal = ((Input) queryTerm).value();
                if (queryTermVal == null) {
                    throw new IllegalArgumentException("cannot use NULL as query term in match predicate");
                }
                if (queryTerm.valueType().equals(DataTypes.GEO_SHAPE)) {
                    return geoMatch(context, arguments, queryTermVal);
                }
                return stringMatch(context, arguments, queryTermVal);
            }

            private Query geoMatch(Context context, List<Symbol> arguments, Object queryTerm) {

                Map fields = (Map) ((Literal) arguments.get(0)).value();
                String fieldName = ((String) Iterables.getOnlyElement(fields.keySet()));
                MappedFieldType fieldType = context.mapperService.smartNameFieldType(fieldName);
                GeoShapeFieldMapper.GeoShapeFieldType geoShapeFieldType = (GeoShapeFieldMapper.GeoShapeFieldType) fieldType;
                String matchType = ((BytesRef) ((Input) arguments.get(2)).value()).utf8ToString();
                @SuppressWarnings("unchecked")
                Shape shape = GeoJSONUtils.map2Shape((Map<String, Object>) queryTerm);

                ShapeRelation relation = ShapeRelation.getRelationByName(matchType);
                assert relation != null : "invalid matchType: " + matchType;

                PrefixTreeStrategy prefixTreeStrategy = geoShapeFieldType.defaultStrategy();
                if (relation == ShapeRelation.DISJOINT) {
                    /**
                     * See {@link org.elasticsearch.index.query.GeoShapeQueryParser}:
                     */
                    // this strategy doesn't support disjoint anymore: but it did before, including creating lucene fieldcache (!)
                    // in this case, execute disjoint as exists && !intersects

                    BooleanQuery.Builder bool = new BooleanQuery.Builder();

                    Query exists = new TermRangeQuery(fieldName, null, null, true, true);

                    Query intersects = prefixTreeStrategy.makeQuery(getArgs(shape, ShapeRelation.INTERSECTS));
                    bool.add(exists, BooleanClause.Occur.MUST);
                    bool.add(intersects, BooleanClause.Occur.MUST_NOT);
                    return new ConstantScoreQuery(bool.build());
                }

                SpatialArgs spatialArgs = getArgs(shape, relation);
                return prefixTreeStrategy.makeQuery(spatialArgs);
            }

            private SpatialArgs getArgs(Shape shape, ShapeRelation relation) {
                switch (relation) {
                    case INTERSECTS:
                        return new SpatialArgs(SpatialOperation.Intersects, shape);
                    case DISJOINT:
                        return new SpatialArgs(SpatialOperation.IsDisjointTo, shape);
                    case WITHIN:
                        return new SpatialArgs(SpatialOperation.IsWithin, shape);
                }
                throw invalidMatchType(relation.getRelationName());
            }

            private AssertionError invalidMatchType(String matchType) {
                throw new AssertionError(String.format(Locale.ENGLISH,
                    "Invalid match type: %s. Analyzer should have made sure that it is valid", matchType));
            }

            private Query stringMatch(Context context, List<Symbol> arguments, Object queryTerm) throws IOException {

                @SuppressWarnings("unchecked")
                Map<String, Object> fields = (Map) ((Literal) arguments.get(0)).value();
                BytesRef queryString = (BytesRef) queryTerm;
                BytesRef matchType = (BytesRef) ((Literal) arguments.get(2)).value();
                //noinspection unchecked
                Map<String, Object> options = (Map<String, Object>) ((Literal) arguments.get(3)).value();

                MatchOptionsAnalysis.validate(options);

                MatchQueryBuilder queryBuilder;
                if (fields.size() == 1) {
                    queryBuilder = new MatchQueryBuilder(context.mapperService, matchType, options);
                } else {
                    queryBuilder = new MultiMatchQueryBuilder(context.mapperService, matchType, options);
                }
                return queryBuilder.query(fields, queryString);
            }
        }

        static class RegexpMatchQuery extends CmpQuery {

            private Query toLuceneRegexpQuery(String fieldName, BytesRef value, Context context) {
                return new ConstantScoreQuery(
                    new RegexpQuery(new Term(fieldName, value), RegExp.ALL));
            }

            @Override
            public Query apply(Function input, Context context) throws IOException {
                Tuple<Reference, Literal> prepare = prepare(input);
                if (prepare == null) {
                    return null;
                }
                String fieldName = prepare.v1().ident().columnIdent().fqn();
                Object value = prepare.v2().value();

                if (value instanceof BytesRef) {
                    BytesRef pattern = ((BytesRef) value);
                    if (isPcrePattern(pattern.utf8ToString())) {
                        return new RegexQuery(new Term(fieldName, pattern));
                    } else {
                        return toLuceneRegexpQuery(fieldName, pattern, context);
                    }
                }
                throw new IllegalArgumentException("Can only use ~ with patterns of type string");
            }
        }

        static class RegexMatchQueryCaseInsensitive extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) throws IOException {
                Tuple<Reference, Literal> prepare = prepare(input);
                if (prepare == null) {
                    return null;
                }
                String fieldName = prepare.v1().ident().columnIdent().fqn();
                Object value = prepare.v2().value();

                if (value instanceof BytesRef) {
                    RegexQuery query = new RegexQuery(new Term(fieldName, BytesRefs.toBytesRef(value)));
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
         * Has only a single method {@link #apply(io.crate.analyze.symbol.Function, io.crate.analyze.symbol.Function, io.crate.lucene.LuceneQueryBuilder.Context)}
         * <p>
         * e.g. in a query like
         * <pre>
         *     where distance(p1, 'POINT (10 20)') = 20
         * </pre>
         * <p>
         * The first parameter (parent) would be the "eq" function.
         * The second parameter (inner) would be the "distance" function.
         * <p>
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
                    return Queries.not(query);
                } else {
                    return query;
                }
            }

            private Query getQuery(Function inner, Context context) {
                RefLiteralPair innerPair = new RefLiteralPair(inner);
                if (!innerPair.isValid()) {
                    return null;
                }
                if (innerPair.reference().valueType().equals(DataTypes.GEO_SHAPE)) {
                    // we have within('POINT(0 0)', shape_column)
                    return genericFunctionFilter(inner, context);
                }
                GeoPointFieldMapper.GeoPointFieldType geoPointFieldType = getGeoPointFieldType(
                    innerPair.reference().ident().columnIdent().fqn(),
                    context.mapperService);

                Map<String, Object> geoJSON = (Map<String, Object>) innerPair.input().value();
                Shape shape = GeoJSONUtils.map2Shape(geoJSON);
                Geometry geometry = JtsSpatialContext.GEO.getGeometryFrom(shape);
                IndexGeoPointFieldData fieldData = context.fieldDataService.getForField(geoPointFieldType);
                if (geometry.isRectangle()) {
                    return getBoundingBoxQuery(shape, fieldData);
                } else {
                    return getPolygonQuery(context, geometry, fieldData);
                }
            }

            private Query getPolygonQuery(Context context, Geometry geometry, IndexGeoPointFieldData fieldData) {
                final Version indexCreated = Version.indexCreated(context.indexCache.indexSettings());
                Coordinate[] coordinates = geometry.getCoordinates();
                final Query query;
                // We can check for version 2.3 here because there is no Crate release with version 2.2
                // although the optimized distance query is available since 2.2
                if (indexCreated.before(Version.V_2_3_0)) {
                    GeoPoint[] points = new GeoPoint[coordinates.length];
                    for (int i = 0; i < coordinates.length; i++) {
                        Coordinate coordinate = coordinates[i];
                        points[i] = new GeoPoint(coordinate.y, coordinate.x);
                    }
                    query = new GeoPolygonQuery(fieldData, points);
                } else {
                    // close the polygon shape if startpoint != endpoint
                    if (!CoordinateArrays.isRing(coordinates)) {
                        coordinates = Arrays.copyOf(coordinates, coordinates.length + 1);
                        coordinates[coordinates.length - 1] = coordinates[0];
                    }
                    final double[] lats = new double[coordinates.length];
                    final double[] lons = new double[coordinates.length];
                    for (int i = 0; i < coordinates.length; i++) {
                        lats[i] = coordinates[i].y;
                        lons[i] = coordinates[i].x;
                    }
                    query = new GeoPointInPolygonQuery(fieldData.getFieldNames().indexName(),
                        GeoPointField.TermEncoding.PREFIX,
                        lons, lats);
                }
                return query;
            }

            private Query getBoundingBoxQuery(Shape shape, IndexGeoPointFieldData fieldData) {
                Rectangle boundingBox = shape.getBoundingBox();
                return new InMemoryGeoBoundingBoxQuery(
                    new GeoPoint(boundingBox.getMaxY(), boundingBox.getMinX()),
                    new GeoPoint(boundingBox.getMinY(), boundingBox.getMaxX()),
                    fieldData
                );
            }

            @Override
            public Query apply(Function input, Context context) throws IOException {
                return getQuery(input, context);
            }
        }

        static class DistanceQuery implements InnerFunctionToQuery {

            final static GeoDistance GEO_DISTANCE = GeoDistance.DEFAULT;
            final static String OPTIMIZE_BOX = "memory";


            /**
             * @param parent the outer function. E.g. in the case of
             *               <pre>where distance(p1, POINT (10 20)) > 20</pre>
             *               this would be
             *               <pre>gt( \<inner function\>,  20)</pre>
             * @param inner  has to be the distance function
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

                String fieldName = distanceRefLiteral.reference().ident().columnIdent().fqn();
                GeoPointFieldMapper.GeoPointFieldType geoPointFieldType = getGeoPointFieldType(fieldName, context.mapperService);
                IndexGeoPointFieldData fieldData = context.fieldDataService.getForField(geoPointFieldType);

                Input geoPointInput = distanceRefLiteral.input();
                Double[] pointValue = (Double[]) geoPointInput.value();
                double lat = pointValue[1];
                double lon = pointValue[0];

                String parentName = functionLiteralPair.functionName();

                GeoPoint geoPoint = new GeoPoint(lat, lon);
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
                        includeLower = true; // ignored by old query if from == null
                        to = distance;
                        break;
                    case LtOperator.NAME:
                        to = distance;
                        includeLower = true; // ignored by old query if from == null
                        break;
                    case GteOperator.NAME:
                        from = distance;
                        includeLower = true;
                        includeUpper = true; // ignored by old query if to == null
                        break;
                    case GtOperator.NAME:
                        from = distance;
                        includeUpper = true; // ignored by old query if to == null
                        break;
                    default:
                        // invalid operator? give up
                        return null;
                }

                final Version indexCreated = Version.indexCreated(context.indexCache.indexSettings());
                final Query query;
                if (indexCreated.before(Version.V_2_3_0)) {
                    query = new GeoDistanceRangeQuery(
                        geoPoint,
                        from,
                        to,
                        includeLower,
                        includeUpper,
                        GEO_DISTANCE,
                        geoPointFieldType,
                        fieldData,
                        OPTIMIZE_BOX);
                } else {
                    GeoUtils.normalizePoint(geoPoint);
                    if (from == null) {
                        from = 0d;
                    }
                    if (to == null) {
                        to = GeoDistanceUtils.maxRadialDistanceMeters(geoPoint.lon(), geoPoint.lat());
                    }
                    query = new GeoPointDistanceRangeQuery(
                        fieldData.getFieldNames().indexName(),
                        GeoPointField.TermEncoding.PREFIX,
                        geoPoint.lon(), geoPoint.lat(),
                        (includeLower) ? from : from + TOLERANCE,
                        (includeUpper) ? to : to - TOLERANCE);
                }
                return query;
            }
        }

        private static GeoPointFieldMapper.GeoPointFieldType getGeoPointFieldType(String fieldName, MapperService mapperService) {
            MappedFieldType fieldType = mapperService.smartNameFieldType(fieldName);
            if (fieldType == null) {
                throw new IllegalArgumentException(String.format("column \"%s\" doesn't exist", fieldName));
            }
            if (!(fieldType instanceof GeoPointFieldMapper.GeoPointFieldType)) {
                throw new IllegalArgumentException(String.format("column \"%s\" isn't of type geo_point", fieldName));
            }
            return (GeoPointFieldMapper.GeoPointFieldType) fieldType;
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
            function = rewriteAndValidateFields(function, context);

            FunctionToQuery toQuery = functions.get(function.info().ident().name());
            if (toQuery == null) {
                return genericFunctionFilter(function, context);
            }

            Query query;
            try {
                query = toQuery.apply(function, context);
            } catch (IOException e) {
                throw ExceptionsHelper.convertToRuntime(e);
            } catch (UnsupportedOperationException e) {
                return genericFunctionFilter(function, context);
            }
            if (query == null) {
                query = queryFromInnerFunction(function, context);
                if (query == null) {
                    return genericFunctionFilter(function, context);
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
                            Query query = functionToQuery.apply(function, (Function) symbol, context);
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
                String columnName = ((Reference) left).ident().columnIdent().name();
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
        private Function rewriteAndValidateFields(Function function, Context context) {
            if (function.arguments().size() == 2) {
                Symbol left = function.arguments().get(0);
                Symbol right = function.arguments().get(1);
                if (left.symbolType() == SymbolType.REFERENCE && right.symbolType().isValueSymbol()) {
                    Reference ref = (Reference) left;
                    if (ref.ident().columnIdent().equals(DocSysColumns.ID)) {
                        function.setArgument(0, DocSysColumns.forTable(ref.ident().tableIdent(), DocSysColumns.UID));
                        function.setArgument(1, Literal.of(Uid.createUid(Constants.DEFAULT_MAPPING_TYPE,
                            ValueSymbolVisitor.STRING.process(right))));
                    } else {
                        String unsupportedMessage = context.unsupportedMessage(ref.ident().columnIdent().name());
                        if (unsupportedMessage != null) {
                            throw new UnsupportedFeatureException(unsupportedMessage);
                        }
                    }
                }
            }
            return function;
        }

        private static Query genericFunctionFilter(Function function, Context context) {
            if (function.valueType() != DataTypes.BOOLEAN) {
                raiseUnsupported(function);
            }
            // avoid field-cache
            // reason1: analyzed columns or columns with index off wouldn't work
            //   substr(n, 1, 1) in the case of n => analyzed would throw an error because n would be an array
            // reason2: would have to load each value into the field cache
            function = (Function) DocReferenceConverter.convertIf(function);

            final CollectInputSymbolVisitor.Context ctx = context.inputSymbolVisitor.extractImplementations(function);
            assert ctx.topLevelInputs().size() == 1;
            @SuppressWarnings("unchecked")
            final Input<Boolean> condition = (Input<Boolean>) ctx.topLevelInputs().get(0);
            @SuppressWarnings("unchecked")
            final List<LuceneCollectorExpression> expressions = ctx.docLevelExpressions();
            final CollectorContext collectorContext = new CollectorContext(
                context.mapperService,
                context.fieldDataService,
                new CollectorFieldsVisitor(expressions.size())
            );

            for (LuceneCollectorExpression expression : expressions) {
                expression.startCollect(collectorContext);
            }
            return new GenericFunctionQuery(function, expressions, collectorContext, condition);
        }


        private static Query raiseUnsupported(Function function) {
            throw new UnsupportedOperationException(
                SymbolFormatter.format("Cannot convert function %s into a query", function));
        }

        @Override
        public Query visitReference(Reference symbol, Context context) {
            // called for queries like: where boolColumn
            if (symbol.valueType() == DataTypes.BOOLEAN) {
                return QueryBuilderHelper.forType(DataTypes.BOOLEAN).eq(symbol.ident().columnIdent().fqn(), true);
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
}
