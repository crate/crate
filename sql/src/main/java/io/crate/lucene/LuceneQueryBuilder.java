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

import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
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
import io.crate.types.DataTypes;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.docset.MatchDocIdSet;
import org.elasticsearch.common.lucene.search.NotFilter;
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

public class LuceneQueryBuilder {

    private final Visitor visitor;

    public LuceneQueryBuilder(Functions functions, SearchContext searchContext, IndexCache indexCache) {
        CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor =
                new CollectInputSymbolVisitor<>(functions, LuceneDocLevelReferenceResolver.INSTANCE);
        visitor = new Visitor(inputSymbolVisitor, indexCache);
        visitor.searchContext = searchContext;
    }

    public Context convert(WhereClause whereClause) {
        Context ctx = new Context();
        if (whereClause.noMatch()) {
            ctx.query = Queries.newMatchNoDocsQuery();
        } else if (!whereClause.hasQuery()) {
            ctx.query = Queries.newMatchAllQuery();
        } else {
            ctx.query = visitor.process(whereClause.query(), ctx);
        }
        return ctx;
    }

    public static class Context {
        Query query;

        final Map<String, Object> filteredFieldValues = new HashMap<>();

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


    static class Visitor extends SymbolVisitor<Context, Query> {

        private SearchContext searchContext;
        private final CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor;
        private final IndexCache indexCache;

        public Visitor(CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor,
                       IndexCache indexCache) {
            this.inputSymbolVisitor = inputSymbolVisitor;
            this.indexCache = indexCache;
        }

        interface FunctionToQuery {

            @Nullable
            public Query apply (Function input, Context context) throws IOException;
        }

        abstract class CmpQuery implements FunctionToQuery {

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

        /**
         * 1 != any ( col ) -->  gt 1 or lt 1
         */
        class AnyNeqQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> tuple = prepare(input);
                if (tuple == null) {
                    return null;
                }
                Reference reference = tuple.v1();
                Object value = tuple.v2().value();

                String columnName = reference.info().ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(reference.valueType());
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
        }

        class AnyNotLikeQuery extends CmpQuery {

            private String negateWildcard(String wildCard) {
                return String.format("~(%s)", wildCard);
            }

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> prepare = prepare(input);
                if (prepare == null) {
                    return null;
                }
                String notLike = negateWildcard(
                        convertWildcardToRegex(BytesRefs.toString(prepare.v2().value())));

                return new RegexpQuery(new Term(
                        prepare.v1().info().ident().columnIdent().fqn(),
                        notLike),
                        RegexpFlag.COMPLEMENT.value()
                );
            }
        }
        class LikeQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> tuple = prepare(input);
                if (tuple == null) {
                    return null;
                }
                String columnName = tuple.v1().info().ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.like(columnName, tuple.v2().value());
            }
        }

        class InQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) throws IOException {
                Tuple<Reference, Literal> tuple = prepare(input);
                if (tuple == null) {
                    return null;
                }
                String field = tuple.v1().info().ident().columnIdent().fqn();
                Literal literal = tuple.v2();
                CollectionType dataType = ((CollectionType) literal.valueType());
                QueryBuilderHelper builder = QueryBuilderHelper.forType(dataType.innerType());
                BooleanQuery booleanQuery = new BooleanQuery();
                Set values = (Set) literal.value();
                for (Object value : values) {
                    booleanQuery.add(builder.eq(field, value), BooleanClause.Occur.SHOULD);
                }
                return booleanQuery;
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

        class IsNullQuery implements FunctionToQuery {

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
                return new FilteredQuery(
                        Queries.newMatchAllQuery(),
                        new NotFilter(builderHelper.rangeFilter(columnName, null, null, true, true)));
            }
        }

        class EqQuery extends CmpQuery {
            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> tuple = super.prepare(input);
                if (tuple == null) {
                    return null;
                }

                String columnName = tuple.v1().info().ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.eq(columnName, tuple.v2().value());
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

        class LtQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> tuple = super.prepare(input);
                if (tuple == null) {
                    return null;
                }

                String columnName = tuple.v1().info().ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.rangeQuery(columnName, null, tuple.v2().value(), false, false);
            }
        }

        class LteQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> tuple = super.prepare(input);
                if (tuple == null) {
                    return null;
                }

                String columnName = tuple.v1().info().ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.rangeQuery(columnName, null, tuple.v2().value(), false, true);
            }
        }

        class GtQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> tuple = super.prepare(input);
                if (tuple == null) {
                    return null;
                }

                String columnName = tuple.v1().info().ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.rangeQuery(columnName, tuple.v2().value(), null, false, false);
            }
        }

        class GteQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> tuple = super.prepare(input);
                if (tuple == null) {
                    return null;
                }

                String columnName = tuple.v1().info().ident().columnIdent().fqn();
                QueryBuilderHelper builder = QueryBuilderHelper.forType(tuple.v1().valueType());
                return builder.rangeQuery(columnName, tuple.v2().value(), null, true, false);
            }
        }

        class ToMatchQuery implements FunctionToQuery {

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
                    queryBuilder = new MatchQueryBuilder(searchContext, indexCache, matchType, options);
                } else {
                    queryBuilder = new MultiMatchQueryBuilder(searchContext, indexCache, matchType, options);
                }
                return queryBuilder.query(fields, queryString);
            }
        }

        class RegexpMatchQuery extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) throws IOException {
                Tuple<Reference, Literal> prepare = prepare(input);
                if (prepare == null) { return null; }
                String fieldName = prepare.v1().info().ident().columnIdent().fqn();
                Object value = prepare.v2().value();
                if (value instanceof String) {
                    return new RegexpQuery(new Term(fieldName, (String) value), RegExp.ALL);
                }
                if (value instanceof BytesRef) {
                    return new RegexpQuery(new Term(fieldName, (BytesRef) value), RegExp.ALL);
                }
                throw new IllegalArgumentException("Can only use ~ with patterns of type string");
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
            public Query apply(Function parent, Function inner, Context context) throws IOException;
        }

        /**
         * for where within(shape1, shape2) = [ true | false ]
         */
        class WithinQuery implements FunctionToQuery, InnerFunctionToQuery {

            @Override
            public Query apply(Function parent, Function inner, Context context) throws IOException {
                FunctionLiteralPair outerPair = new FunctionLiteralPair(parent);
                if (!outerPair.isValid()) {
                    return null;
                }
                Query query = getQuery(inner);
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

            private Query getQuery(Function inner) {
                RefLiteralPair innerPair = new RefLiteralPair(inner);
                if (!innerPair.isValid()) {
                    return null;
                }
                GeoPointFieldMapper mapper = getGeoPointFieldMapper(innerPair.reference().info().ident().columnIdent().fqn());
                Shape shape = (Shape) innerPair.input().value();
                Geometry geometry = JtsSpatialContext.GEO.getGeometryFrom(shape);
                IndexGeoPointFieldData fieldData = searchContext.fieldData().getForField(mapper);
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
                return new FilteredQuery(Queries.newMatchAllQuery(), indexCache.filter().cache(filter));
            }

            @Override
            public Query apply(Function input, Context context) throws IOException {
                return getQuery(input);
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
                FieldMapper mapper = getGeoPointFieldMapper(fieldName);
                GeoPointFieldMapper geoMapper = ((GeoPointFieldMapper) mapper);
                IndexGeoPointFieldData fieldData = searchContext.fieldData().getForField(mapper);

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
                return new FilteredQuery(Queries.newMatchAllQuery(), indexCache.filter().cache(filter));
            }
        }

        private GeoPointFieldMapper getGeoPointFieldMapper(String fieldName) {
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

        private final EqQuery eqQuery = new EqQuery();
        private final LtQuery ltQuery = new LtQuery();
        private final LteQuery lteQuery = new LteQuery();
        private final GtQuery gtQuery = new GtQuery();
        private final GteQuery gteQuery = new GteQuery();
        private final LikeQuery likeQuery = new LikeQuery();
        private final WithinQuery withinQuery = new WithinQuery();
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
                        .put(LikeOperator.NAME, likeQuery)
                        .put(InOperator.NAME, new InQuery())
                        .put(NotPredicate.NAME, new NotQuery())
                        .put(IsNullPredicate.NAME, new IsNullQuery())
                        .put(MatchPredicate.NAME, new ToMatchQuery())
                        .put(AnyEqOperator.NAME, eqQuery)
                        .put(AnyNeqOperator.NAME, new AnyNeqQuery())
                        .put(AnyLtOperator.NAME, ltQuery)
                        .put(AnyLteOperator.NAME, lteQuery)
                        .put(AnyGteOperator.NAME, gteQuery)
                        .put(AnyGtOperator.NAME, gtQuery)
                        .put(AnyLikeOperator.NAME, likeQuery)
                        .put(AnyNotLikeOperator.NAME, new AnyNotLikeQuery())
                        .put(RegexpMatchOperator.NAME, new RegexpMatchQuery())
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
                return genericFunctionQuery(function);
            }

            Query query;
            try {
                query = toQuery.apply(function, context);
            } catch (IOException e) {
                throw ExceptionsHelper.convertToRuntime(e);
            } catch (UnsupportedOperationException e) {
                return genericFunctionQuery(function);
            }
            if (query == null) {
                query = queryFromInnerFunction(function, context);
                if (query == null) {
                    return genericFunctionQuery(function);
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

        private Query genericFunctionQuery(Function function) {
            if (function.valueType() != DataTypes.BOOLEAN) {
                raiseUnsupported(function);
            }
            // avoid field-cache
            // reason1: analyzed columns or columns with index off wouldn't work
            //   substr(n, 1, 1) in the case of n => analyzed would throw an error because n would be an array
            // reason2: would have to load each value into the field cache
            DocReferenceConverter.convertIf(function, Predicates.<Reference>alwaysTrue());

            final CollectInputSymbolVisitor.Context ctx = inputSymbolVisitor.process(function);
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
            Filter filter = new Filter() {
                @Override
                public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
                    for (LuceneCollectorExpression expression : expressions) {
                        expression.setNextReader(context);
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
                            acceptDocs);
                }
            };
            Filter cachedFilter = indexCache.filter().cache(filter);
            return new FilteredQuery(Queries.newMatchAllQuery(), cachedFilter);
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

        private Query raiseUnsupported(Function function) {
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
}
