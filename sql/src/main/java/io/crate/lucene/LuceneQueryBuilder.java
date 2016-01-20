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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.spatial4j.core.shape.Shape;
import io.crate.Constants;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.*;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.geo.GeoJSONUtils;
import io.crate.lucene.match.MatchQueryBuilder;
import io.crate.lucene.match.MultiMatchQueryBuilder;
import io.crate.metadata.DocReferenceConverter;
import io.crate.metadata.Functions;
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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.sandbox.queries.regex.JavaUtilRegexCapabilities;
import org.apache.lucene.sandbox.queries.regex.RegexQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
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

import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static io.crate.operation.scalar.regex.RegexMatcher.isPcrePattern;

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
                    assert collectionSymbol instanceof Reference: "no reference found in ANY expression";
                    return applyArrayReference((Reference)collectionSymbol, (Literal)left, context);
                } else if (collectionSymbol.symbolType().isValueSymbol()) {
                    assert left instanceof Reference : "no reference found in ANY expression";
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

            private TermsQuery termQuery(String columnName, Literal arrayLiteral) {
                Object values = arrayLiteral.value();
                if (values instanceof Collection) {
                    return new TermsQuery(columnName,
                            getBytesRefs((Collection)values, TermBuilder.forType(arrayLiteral.valueType())));
                } else  {
                    return new TermsQuery(columnName,
                            getBytesRefs((Object[])values, TermBuilder.forType(arrayLiteral.valueType())));
                }
            }

            @Override
            protected Query applyArrayReference(Reference arrayReference, Literal literal, Context context) throws IOException {
                QueryBuilderHelper builder = QueryBuilderHelper.forType(((CollectionType)arrayReference.valueType()).innerType());
                return builder.eq(arrayReference.ident().columnIdent().fqn(), literal.value());
            }

            @Override
            protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) throws IOException {
                String columnName = reference.ident().columnIdent().fqn();
                return termQuery(columnName, arrayLiteral);
            }
        }

        static class AnyNeqQuery extends AbstractAnyQuery {

            @Override
            protected Query applyArrayReference(Reference arrayReference, Literal literal, Context context) throws IOException {
                // 1 != any ( col ) -->  gt 1 or lt 1
                String columnName = arrayReference.info().ident().columnIdent().fqn();
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
                String columnName = reference.info().ident().columnIdent().fqn();
                QueryBuilderHelper helper = QueryBuilderHelper.forType(reference.valueType());

                BooleanQuery.Builder filter = new BooleanQuery.Builder();
                BooleanQuery.Builder notBuilder = new BooleanQuery.Builder();
                for (Object value : toIterable(arrayLiteral.value())) {
                    notBuilder.add(helper.eq(columnName, value), BooleanClause.Occur.MUST);
                }
                filter.add(notBuilder.build(), BooleanClause.Occur.MUST_NOT);

                return filter.build();
            }
        }

        static class AnyNotLikeQuery extends AbstractAnyQuery {

            @Override
            protected Query applyArrayReference(Reference arrayReference, Literal literal, Context context) throws IOException {
                String regexString = LikeOperator.patternToRegex(BytesRefs.toString(literal.value()), LikeOperator.DEFAULT_ESCAPE, false);
                regexString = regexString.substring(1, regexString.length() - 1);
                String notLike = negateWildcard(regexString);

                return new RegexpQuery(new Term(
                        arrayReference.info().ident().columnIdent().fqn(),
                        notLike),
                        RegexpFlag.COMPLEMENT.value()
                );
            }

            @Override
            protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) throws IOException {
                // col not like ANY (['a', 'b']) --> not(and(like(col, 'a'), like(col, 'b')))
                BooleanQuery.Builder query = new BooleanQuery.Builder();
                BooleanQuery.Builder notQuery = new BooleanQuery.Builder();

                String columnName = reference.ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(reference.valueType());
                for (Object value : toIterable(arrayLiteral.value())) {
                    notQuery.add(builder.like(columnName, value, context.indexCache.query()), BooleanClause.Occur.MUST);
                }
                query.add(notQuery.build(), BooleanClause.Occur.MUST_NOT);
                return query.build();
            }
        }

        static class AnyLikeQuery extends AbstractAnyQuery {

            private final LikeQuery likeQuery = new LikeQuery();

            @Override
            protected Query applyArrayReference(Reference arrayReference, Literal literal, Context context) throws IOException {
                return likeQuery.toQuery(arrayReference, literal.value(), context);
            }

            @Override
            protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) throws IOException {
                // col like ANY (['a', 'b']) --> or(like(col, 'a'), like(col, 'b'))
                BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
                booleanQuery.setMinimumNumberShouldMatch(1);
                for (Object value : toIterable(arrayLiteral.value())) {
                    booleanQuery.add(likeQuery.toQuery(reference, value, context), BooleanClause.Occur.SHOULD);
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

                String columnName = reference.info().ident().columnIdent().fqn();
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
                String field = tuple.v1().info().ident().columnIdent().fqn();
                Literal literal = tuple.v2();
                CollectionType dataType = ((CollectionType) literal.valueType());

                Set values = (Set) literal.value();
                DataType innerType = dataType.innerType();
                BytesRef[] terms = getBytesRefs(values, TermBuilder.forType(innerType));
                return new TermsQuery(field, terms);
            }
        }

        class NotQuery implements FunctionToQuery {

            @Override
            public Query apply(Function input, Context context) {
                assert input != null;
                assert input.arguments().size() == 1;
                BooleanQuery.Builder query = new BooleanQuery.Builder();

                query.add(process(input.arguments().get(0), context), BooleanClause.Occur.MUST_NOT);
                query.add(Queries.newMatchAllQuery(), BooleanClause.Occur.MUST);

                return query.build();
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

                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                builder.add(builderHelper.rangeQuery(columnName, null, null, true, true), BooleanClause.Occur.MUST_NOT);
                return builder.build();
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
                    BooleanQuery.Builder boolTermsQuery = new BooleanQuery.Builder();
                    DataType type = literal.valueType();
                    while (DataTypes.isCollectionType(type)) {
                        type = ((CollectionType) type).innerType();
                    }
                    QueryBuilderHelper builder = QueryBuilderHelper.forType(type);
                    Object value = literal.value();
                    buildTermsQuery(boolTermsQuery, value, columnName, builder);

                    BooleanQuery booleanQuery = boolTermsQuery.build();
                    if (booleanQuery.clauses().isEmpty()) {
                        // all values are null...
                        return genericFunctionFilter(input, context);
                    }

                    // wrap boolTermsFilter and genericFunction filter in an additional BooleanFilter to control the ordering of the filters
                    // termsFilter is applied first
                    // afterwards the more expensive genericFunctionFilter
                    BooleanQuery.Builder filterClauses = new BooleanQuery.Builder();
                    filterClauses.add(booleanQuery, BooleanClause.Occur.MUST);
                    filterClauses.add(genericFunctionFilter(input, context), BooleanClause.Occur.MUST);
                    return filterClauses.build();
                }
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.eq(columnName, tuple.v2().value());
            }

            private void buildTermsQuery(BooleanQuery.Builder booleanQueryBuilder,
                                         Object value,
                                         String columnName,
                                         QueryBuilderHelper builder) {
                if (value == null) {
                    return;
                }
                if (value.getClass().isArray()) {
                    Object[] array = (Object[]) value;
                    for (Object o : array) {
                        buildTermsQuery(booleanQueryBuilder, o, columnName, builder);
                    }
                } else {
                    booleanQueryBuilder.add(builder.eq(columnName, value), BooleanClause.Occur.MUST);
                }
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
                        ((CollectionType)arrayReference.valueType()).innerType(),
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
                assert Symbol.isLiteral(arguments.get(0), DataTypes.OBJECT); // fields
                assert Symbol.isLiteral(arguments.get(2), DataTypes.STRING); // matchType

                Symbol queryTerm = arguments.get(1);
                if (queryTerm.valueType().equals(DataTypes.GEO_SHAPE)) {
                    return geoMatch(context, arguments, queryTerm);
                }
                return stringMatch(context, arguments, queryTerm);
            }

            private Query geoMatch(Context context, List<Symbol> arguments, Symbol queryTerm) {
                assert Symbol.isLiteral(arguments.get(1), DataTypes.GEO_SHAPE);

                Map fields = (Map) ((Literal) arguments.get(0)).value();
                String fieldName = ((String) Iterables.getOnlyElement(fields.keySet()));
                MappedFieldType fieldType = context.mapperService.smartNameFieldType(fieldName);
                GeoShapeFieldMapper.GeoShapeFieldType geoShapeFieldType = (GeoShapeFieldMapper.GeoShapeFieldType) fieldType;
                String matchType = ((BytesRef) ((Input) arguments.get(2)).value()).utf8ToString();
                @SuppressWarnings("unchecked")
                Shape shape = GeoJSONUtils.map2Shape(((Map<String, Object>) ((Input) queryTerm).value()));

                ShapeRelation relation = ShapeRelation.getRelationByName(matchType);
                assert relation != null : "invalid matchType: " + matchType;
                SpatialArgs spatialArgs = getArgs(shape, relation);
                return geoShapeFieldType.defaultStrategy().makeQuery(spatialArgs);
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

            private Query stringMatch(Context context, List<Symbol> arguments, Symbol queryTerm) throws IOException {
                assert Symbol.isLiteral(queryTerm, DataTypes.STRING);
                assert Symbol.isLiteral(arguments.get(3), DataTypes.OBJECT);

                @SuppressWarnings("unchecked")
                Map<String, Object> fields = (Map) ((Literal) arguments.get(0)).value();
                BytesRef queryString = (BytesRef) ((Literal) queryTerm).value();
                BytesRef matchType = (BytesRef) ((Literal) arguments.get(2)).value();
                Map options = (Map) ((Literal) arguments.get(3)).value();

                checkArgument(queryString != null, "cannot use NULL as query term in match predicate");

                MatchQueryBuilder queryBuilder;
                if (fields.size() == 1) {
                    queryBuilder = new MatchQueryBuilder(context.mapperService, context.indexCache, matchType, options);
                } else {
                    queryBuilder = new MultiMatchQueryBuilder(context.mapperService, context.indexCache, matchType, options);
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
                if (prepare == null) { return null; }
                String fieldName = prepare.v1().info().ident().columnIdent().fqn();
                Object value = prepare.v2().value();

                if (value instanceof BytesRef) {
                    BytesRef pattern = ((BytesRef) value);
                    if (isPcrePattern(pattern)) {
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
                if (prepare == null) { return null; }
                String fieldName = prepare.v1().info().ident().columnIdent().fqn();
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
                    BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
                    booleanQuery.add(query, BooleanClause.Occur.MUST_NOT);
                    return booleanQuery.build();
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
                // TODO: FIX ME! goe not working yet due to smartMapper missing
                /*GeoPointFieldMapper mapper = getGeoPointFieldMapper(
                        innerPair.reference().info().ident().columnIdent().fqn(),
                        context.mapperService
                );
                Map<String, Object> geoJSON = (Map<String, Object>) innerPair.input().value();
                Shape shape = GeoJSONUtils.map2Shape(geoJSON);
                Geometry geometry = JtsSpatialContext.GEO.getGeometryFrom(shape);
                IndexGeoPointFieldData fieldData = context.fieldDataService.getForField(mapper);
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
                return new FilteredQuery(Queries.newMatchAllQuery(), context.indexCache.filter().cache(filter));*/
                return null;
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
                MappedFieldType mapper = getGeoPointFieldType(fieldName, context.mapperService);
                IndexGeoPointFieldData fieldData = context.fieldDataService.getForField(mapper);

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

                // TODO: FIX ME!
                // GeoPointFieldMapper geoMapper = ((GeoPointFieldMapper) mapper);
                /*Filter filter = new GeoDistanceRangeFilter(
                        geoPoint,
                        from,
                        to,
                        includeLower,
                        includeUpper,
                        GEO_DISTANCE,
                        geoMapper,
                        fieldData,
                        OPTIMIZE_BOX
                );
                return new FilteredQuery(Queries.newMatchAllQuery(), context.indexCache.filter().cache(filter));*/
                return null;
            }
        }

        private static GeoPointFieldMapper.GeoPointFieldType getGeoPointFieldType(String fieldName, MapperService mapperService) {
            MappedFieldType fieldType =  mapperService.smartNameFieldType(fieldName);
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
        private Function rewriteAndValidateFields(Function function, Context context) {
            if (function.arguments().size() == 2) {
                Symbol left = function.arguments().get(0);
                Symbol right = function.arguments().get(1);
                if (left.symbolType() == SymbolType.REFERENCE && right.symbolType().isValueSymbol()) {
                    Reference ref = (Reference) left;
                    if (ref.info().ident().columnIdent().equals(DocSysColumns.ID)) {
                        function.setArgument(0,
                                new Reference(DocSysColumns.forTable(ref.ident().tableIdent(), DocSysColumns.UID)));
                        function.setArgument(1, Literal.newLiteral(Uid.createUid(Constants.DEFAULT_MAPPING_TYPE,
                                ValueSymbolVisitor.STRING.process(right))));
                    } else {
                        String unsupportedMessage = context.unsupportedMessage(ref.info().ident().columnIdent().name());
                        if (unsupportedMessage != null) {
                            throw new UnsupportedFeatureException(unsupportedMessage);
                        }
                    }
                }
            }
            return function;
        }

        private static Filter genericFunctionFilter(Function function, Context context) {
            if (function.valueType() != DataTypes.BOOLEAN) {
                raiseUnsupported(function);
            }
            // avoid field-cache
            // reason1: analyzed columns or columns with index off wouldn't work
            //   substr(n, 1, 1) in the case of n => analyzed would throw an error because n would be an array
            // reason2: would have to load each value into the field cache
            function = (Function)DocReferenceConverter.convertIf(function);

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
            return new FunctionFilter(expressions, collectorContext, condition);
        }

        public static class FunctionFilter extends Filter {

            private final List<LuceneCollectorExpression> expressions;
            private final CollectorContext collectorContext;
            private final Input<Boolean> condition;

            public FunctionFilter(List<LuceneCollectorExpression> expressions, CollectorContext collectorContext, Input<Boolean> condition) {
                this.expressions = expressions;
                this.collectorContext = collectorContext;
                this.condition = condition;
            }

            @Override
            public DocIdSet getDocIdSet(final LeafReaderContext context, Bits acceptDocs) throws IOException {
                for (LuceneCollectorExpression expression : expressions) {
                    expression.setNextReader(context.reader().getContext());
                }
                DocIdSet docIdSet = new DocIdSet() {

                    @Override
                    public long ramBytesUsed() {
                        return 0; // FIXME
                    }

                    @Override
                    public DocIdSetIterator iterator() throws IOException {
                        return DocIdSetIterator.all(context.reader().maxDoc());
                    }
                };
                return BitsFilteredDocIdSet.wrap(
                        new FunctionDocSet(
                                context.reader(),
                                collectorContext.visitor(),
                                condition,
                                expressions,
                                docIdSet
                        ),
                        acceptDocs
                );
            }

            @Override
            public String toString(String field) {
                return field;
            }
        }

        static class FunctionDocSet extends FilteredDocIdSet {

            private final LeafReader reader;
            private final CollectorFieldsVisitor fieldsVisitor;
            private final Input<Boolean> condition;
            private final List<LuceneCollectorExpression> expressions;
            private final boolean fieldsVisitorEnabled;

            protected FunctionDocSet(LeafReader reader,
                                     @Nullable CollectorFieldsVisitor fieldsVisitor,
                                     Input<Boolean> condition,
                                     List<LuceneCollectorExpression> expressions,
                                     DocIdSet docIdSet) {
                super(docIdSet);
                this.reader = reader;
                this.fieldsVisitor = fieldsVisitor;
                //noinspection SimplifiableConditionalExpression
                this.fieldsVisitorEnabled = fieldsVisitor == null ? false : fieldsVisitor.required();
                this.condition = condition;
                this.expressions = expressions;
            }

            @Override
            protected boolean match(int doc) {
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
