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
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateArrays;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import io.crate.Constants;
import io.crate.analyze.MatchOptionsAnalysis;
import io.crate.analyze.WhereClause;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.ValueSymbolVisitor;
import io.crate.expression.symbol.format.SymbolFormatter;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.data.Input;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.geo.GeoJSONUtils;
import io.crate.lucene.match.CrateRegexCapabilities;
import io.crate.lucene.match.CrateRegexQuery;
import io.crate.lucene.match.MatchQueries;
import io.crate.metadata.DocReferences;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.expression.InputFactory;
import io.crate.execution.engine.collect.DocInputFactory;
import io.crate.execution.engine.collect.collectors.CollectorFieldsVisitor;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.GtOperator;
import io.crate.expression.operator.GteOperator;
import io.crate.expression.operator.LikeOperator;
import io.crate.expression.operator.LtOperator;
import io.crate.expression.operator.LteOperator;
import io.crate.expression.operator.OrOperator;
import io.crate.expression.operator.RegexpMatchCaseInsensitiveOperator;
import io.crate.expression.operator.RegexpMatchOperator;
import io.crate.expression.operator.any.AnyLikeOperator;
import io.crate.expression.operator.any.AnyOperators;
import io.crate.expression.predicate.IsNullPredicate;
import io.crate.expression.predicate.MatchPredicate;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.scalar.conditional.CoalesceFunction;
import io.crate.expression.scalar.geo.DistanceFunction;
import io.crate.expression.scalar.geo.WithinFunction;
import io.crate.types.ArrayType;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.geopoint.search.GeoPointInPolygonQuery;
import org.apache.lucene.spatial.geopoint.search.XGeoPointDistanceRangeQuery;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.BaseGeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.LatLonPointFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.RegexpFlag;
import org.elasticsearch.index.search.geo.GeoPolygonQuery;
import org.elasticsearch.index.search.geo.LegacyInMemoryGeoBoundingBoxQuery;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.crate.lucene.DistanceQueries.esV5DistanceQuery;
import static io.crate.expression.scalar.regex.RegexMatcher.isPcrePattern;
import static org.elasticsearch.common.geo.GeoUtils.TOLERANCE;

@Singleton
public class LuceneQueryBuilder {

    private static final Logger LOGGER = Loggers.getLogger(LuceneQueryBuilder.class);
    private static final Visitor VISITOR = new Visitor();
    private final Functions functions;

    @Inject
    public LuceneQueryBuilder(Functions functions) {
        this.functions = functions;
    }

    public Context convert(WhereClause whereClause,
                           MapperService mapperService,
                           QueryShardContext queryShardContext,
                           IndexFieldDataService indexFieldDataService,
                           IndexCache indexCache) throws UnsupportedFeatureException {
        Context ctx = new Context(functions, mapperService, indexFieldDataService, indexCache, queryShardContext);
        if (whereClause.noMatch()) {
            ctx.query = Queries.newMatchNoDocsQuery("whereClause no-match");
        } else if (!whereClause.hasQuery()) {
            ctx.query = Queries.newMatchAllQuery();
        } else {
            Symbol query = DocReferences.inverseSourceLookup(whereClause.query());
            ctx.query = VISITOR.process(query, ctx);
        }
        if (LOGGER.isTraceEnabled()) {
            if (whereClause.hasQuery()) {
                LOGGER.trace("WHERE CLAUSE [{}] -> LUCENE QUERY [{}] ", SymbolPrinter.INSTANCE.printUnqualified(whereClause.query()), ctx.query);
            }
        }
        return ctx;
    }

    private static Query termsQuery(@Nullable MappedFieldType fieldType, List values, QueryShardContext context) {
        if (fieldType == null) {
            return Queries.newMatchNoDocsQuery("column does not exist in this index");
        }
        return fieldType.termsQuery(values, context);
    }


    private static List asList(Literal literal) {
        Object val = literal.value();
        if (val instanceof Object[]) {
            return Stream.of((Object[]) val).filter(Objects::nonNull).collect(Collectors.toList());
        }
        return (List) val;
    }

    public static class Context {
        Query query;

        final Map<String, Object> filteredFieldValues = new HashMap<>();

        final DocInputFactory docInputFactory;
        final MapperService mapperService;
        final IndexFieldDataService fieldDataService;
        final IndexCache indexCache;
        final QueryShardContext queryShardContext;

        Context(Functions functions,
                MapperService mapperService,
                IndexFieldDataService fieldDataService,
                IndexCache indexCache,
                QueryShardContext queryShardContext) {
            this.queryShardContext = queryShardContext;
            FieldTypeLookup typeLookup = mapperService::fullName;
            this.docInputFactory = new DocInputFactory(
                functions,
                typeLookup,
                new LuceneReferenceResolver(typeLookup, mapperService.getIndexSettings()));
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
        static final Set<String> FILTERED_FIELDS = ImmutableSet.of("_score");

        /**
         * key = columnName
         * value = error message
         * <p/>
         * (in the _version case if the primary key is present a GetPlan is built from the planner and
         * the LuceneQueryBuilder is never used)
         */
        static final Map<String, String> UNSUPPORTED_FIELDS = ImmutableMap.<String, String>builder()
            .put("_version", VersionInvalidException.ERROR_MSG)
            .build();

        @Nullable
        MappedFieldType getFieldTypeOrNull(String fqColumnName) {
            return mapperService.fullName(fqColumnName);
        }

        public QueryShardContext queryShardContext() {
            return queryShardContext;
        }
    }

    public static String convertSqlLikeToLuceneWildcard(String wildcardString) {
        // lucene uses * and ? as wildcard characters
        // but via SQL they are used as % and _
        // here they are converted back.
        StringBuilder regex = new StringBuilder();

        boolean escaped = false;
        for (char currentChar : wildcardString.toCharArray()) {
            if (!escaped && currentChar == LikeOperator.DEFAULT_ESCAPE) {
                escaped = true;
            } else {
                switch (currentChar) {
                    case '%':
                        regex.append(escaped ? '%' : '*');
                        escaped = false;
                        break;
                    case '_':
                        regex.append(escaped ? '_' : '?');
                        escaped = false;
                        break;
                    default:
                        switch (currentChar) {
                            case '\\':
                            case '*':
                            case '?':
                                regex.append('\\');
                                break;
                            default:
                        }
                        regex.append(currentChar);
                        escaped = false;
                }
            }
        }
        return regex.toString();
    }

    static String negateWildcard(String wildCard) {
        return String.format(Locale.ENGLISH, "~(%s)", wildCard);
    }

    static class Visitor extends SymbolVisitor<Context, Query> {

        interface FunctionToQuery {

            @Nullable
            Query apply(Function input, Context context) throws IOException;
        }

        abstract static class CmpQuery implements FunctionToQuery {

            @Nullable
            protected Tuple<Reference, Literal> prepare(Function input) {
                assert input != null : "function must not be null";
                assert input.arguments().size() == 2 : "function's number of arguments must be 2";

                Symbol left = input.arguments().get(0);
                Symbol right = input.arguments().get(1);

                if (!(left instanceof Reference) || !(right.symbolType().isValueSymbol())) {
                    return null;
                }
                assert right.symbolType() == SymbolType.LITERAL :
                    "right.symbolType() must be " + SymbolType.LITERAL;
                return new Tuple<>((Reference) left, (Literal) right);
            }
        }

        abstract static class AbstractAnyQuery implements FunctionToQuery {

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
            static Iterable<?> toIterable(Object value) {
                return Iterables.transform(AnyOperators.collectionValueToIterable(value), new com.google.common.base.Function<Object, Object>() {
                    @Nullable
                    @Override
                    public Object apply(@Nullable Object input) {
                        if (input instanceof String) {
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
            protected Query applyArrayReference(Reference arrayReference, Literal literal, Context context) {
                MappedFieldType fieldType = context.getFieldTypeOrNull(arrayReference.ident().columnIdent().fqn());
                if (fieldType == null) {
                    if (CollectionType.unnest(arrayReference.valueType()).equals(DataTypes.OBJECT)) {
                        return null; // fallback to generic query to enable {x=10} = any(objects)
                    }
                    return Queries.newMatchNoDocsQuery("column doesn't exist in this index");
                }
                return fieldType.termQuery(literal.value(), null);
            }

            @Override
            protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) {
                String columnName = reference.ident().columnIdent().fqn();
                return termsQuery(context.getFieldTypeOrNull(columnName), asList(arrayLiteral), context.queryShardContext);
            }
        }

        static class AnyNeqQuery extends AbstractAnyQuery {

            @Override
            protected Query applyArrayReference(Reference arrayReference, Literal literal, Context context) {
                // 1 != any ( col ) -->  gt 1 or lt 1
                String columnName = arrayReference.ident().columnIdent().fqn();
                Object value = literal.value();

                MappedFieldType fieldType = context.getFieldTypeOrNull(columnName);
                if (fieldType == null) {
                    return Queries.newMatchNoDocsQuery("column does not exist in this index");
                }
                BooleanQuery.Builder query = new BooleanQuery.Builder();
                query.setMinimumNumberShouldMatch(1);
                query.add(
                    fieldType.rangeQuery(value, null, false, false, context.queryShardContext),
                    BooleanClause.Occur.SHOULD
                );
                query.add(
                    fieldType.rangeQuery(null, value, false, false, context.queryShardContext),
                    BooleanClause.Occur.SHOULD
                );
                return query.build();
            }

            @Override
            protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) {
                //  col != ANY ([1,2,3]) --> not(col=1 and col=2 and col=3)
                String columnName = reference.ident().columnIdent().fqn();
                MappedFieldType fieldType = context.getFieldTypeOrNull(columnName);
                if (fieldType == null) {
                    return Queries.newMatchNoDocsQuery("column does not exist in this index");
                }

                BooleanQuery.Builder andBuilder = new BooleanQuery.Builder();
                for (Object value : toIterable(arrayLiteral.value())) {
                    andBuilder.add(fieldType.termQuery(value, null), BooleanClause.Occur.MUST);
                }
                return Queries.not(andBuilder.build());
            }
        }

        static class AnyNotLikeQuery extends AbstractAnyQuery {

            @Override
            protected Query applyArrayReference(Reference arrayReference, Literal literal, Context context) {
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
            protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) {
                // col not like ANY (['a', 'b']) --> not(and(like(col, 'a'), like(col, 'b')))
                String columnName = reference.ident().columnIdent().fqn();
                MappedFieldType fieldType = context.getFieldTypeOrNull(columnName);

                BooleanQuery.Builder andLikeQueries = new BooleanQuery.Builder();
                for (Object value : toIterable(arrayLiteral.value())) {
                    andLikeQueries.add(
                        LikeQueryBuilder.like(reference.valueType(), fieldType, value),
                        BooleanClause.Occur.MUST);
                }
                return Queries.not(andLikeQueries.build());
            }
        }

        static class AnyLikeQuery extends AbstractAnyQuery {

            @Override
            protected Query applyArrayReference(Reference arrayReference, Literal literal, Context context) {
                return LikeQuery.toQuery(arrayReference, literal.value(), context);
            }

            @Override
            protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) {
                // col like ANY (['a', 'b']) --> or(like(col, 'a'), like(col, 'b'))
                BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
                booleanQuery.setMinimumNumberShouldMatch(1);
                for (Object value : toIterable(arrayLiteral.value())) {
                    booleanQuery.add(LikeQuery.toQuery(reference, value, context), BooleanClause.Occur.SHOULD);
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

            static Query toQuery(Reference reference, Object value, Context context) {
                DataType dataType = CollectionType.unnest(reference.valueType());
                return LikeQueryBuilder.like(
                    dataType,
                    context.getFieldTypeOrNull(reference.ident().columnIdent().fqn()),
                    value
                );
            }
        }

        class NotQuery implements FunctionToQuery {

            private class SymbolToNotNullContext {
                private final HashSet<Reference> references = new HashSet<>();
                boolean hasStrictThreeValuedLogicFunction = false;

                boolean add(Reference symbol) {
                    return references.add(symbol);
                }

                Set<Reference> references() {
                    return references;
                }
            }

            private class SymbolToNotNullRangeQueryArgs extends SymbolVisitor<SymbolToNotNullContext, Void> {

                /**
                 * Three valued logic systems are defined in logic as systems in which there are 3 truth values: true,
                 * false and an indeterminate third value (in our case null is the third value).
                 * <p>
                 * This is a set of functions for which inputs evaluating to null needs to be explicitly included or
                 * excluded (in the case of 'not ...') in the boolean queries
                 */
                private final Set<String> STRICT_3VL_FUNCTIONS =
                    ImmutableSet.of(
                        AnyOperators.Names.EQ,
                        AnyOperators.Names.NEQ,
                        AnyOperators.Names.GTE,
                        AnyOperators.Names.GT,
                        AnyOperators.Names.LTE,
                        AnyOperators.Names.LT,
                        AnyLikeOperator.LIKE,
                        AnyLikeOperator.NOT_LIKE,
                        CoalesceFunction.NAME);

                private boolean isStrictThreeValuedLogicFunction(Function symbol) {
                    return STRICT_3VL_FUNCTIONS.contains(symbol.info().ident().name());
                }

                @Override
                public Void visitReference(Reference symbol, SymbolToNotNullContext context) {
                    context.add(symbol);
                    return null;
                }

                @Override
                public Void visitFunction(Function symbol, SymbolToNotNullContext context) {
                    if (!isStrictThreeValuedLogicFunction(symbol)) {
                        for (Symbol arg : symbol.arguments()) {
                            process(arg, context);
                        }
                    } else {
                        context.hasStrictThreeValuedLogicFunction = true;
                    }
                    return null;
                }
            }

            private final SymbolToNotNullRangeQueryArgs INNER_VISITOR = new SymbolToNotNullRangeQueryArgs();

            @Override
            public Query apply(Function input, Context context) {
                assert input != null : "function must not be null";
                assert input.arguments().size() == 1 : "function's number of arguments must be 1";
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
                    MappedFieldType fieldType = context.getFieldTypeOrNull(columnName);
                    if (fieldType == null) {
                        // probably an object column, fallback to genericFunctionFilter
                        return null;
                    }
                    if (reference.isNullable()) {
                        builder.add(ExistsQueryBuilder.newFilter(context.queryShardContext, columnName), BooleanClause.Occur.MUST);
                    }
                }
                if (ctx.hasStrictThreeValuedLogicFunction) {
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
                assert input != null : "input must not be null";
                assert input.arguments().size() == 1 : "function's number of arguments must be 1";
                Symbol arg = input.arguments().get(0);
                if (arg.symbolType() != SymbolType.REFERENCE) {
                    return null;
                }
                Reference reference = (Reference) arg;
                String columnName = reference.ident().columnIdent().fqn();
                return Queries.not(ExistsQueryBuilder.newFilter(context.queryShardContext, columnName));
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
                MappedFieldType fieldType = context.getFieldTypeOrNull(columnName);
                if (fieldType == null) {
                    if (reference.valueType().equals(DataTypes.OBJECT)) {
                        return null; // fallback to generic filter for  "o = {x=10, y=20}"
                    }
                    // field doesn't exist, can't match
                    return Queries.newMatchNoDocsQuery("column does not exist in this index");
                }
                if (DataTypes.isCollectionType(reference.valueType()) &&
                    DataTypes.isCollectionType(literal.valueType())) {

                    List values = asList(literal);
                    if (values.isEmpty()) {
                        return genericFunctionFilter(input, context);
                    }
                    Query termsQuery = termsQuery(fieldType, values, context.queryShardContext);

                    // wrap boolTermsFilter and genericFunction filter in an additional BooleanFilter to control the ordering of the filters
                    // termsFilter is applied first
                    // afterwards the more expensive genericFunctionFilter
                    BooleanQuery.Builder filterClauses = new BooleanQuery.Builder();
                    filterClauses.add(termsQuery, BooleanClause.Occur.MUST);
                    filterClauses.add(genericFunctionFilter(input, context), BooleanClause.Occur.MUST);
                    return filterClauses.build();
                }
                return fieldType.termQuery(literal.value(), context.queryShardContext);
            }
        }

        class AndQuery implements FunctionToQuery {
            @Override
            public Query apply(Function input, Context context) {
                assert input != null : "input must not be null";
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
                assert input != null : "input must not be null";
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
            protected Query applyArrayReference(Reference arrayReference, Literal literal, Context context) {
                // 1 < ANY (array_col) --> array_col > 1
                return rangeQuery.toQuery(
                    arrayReference,
                    literal.value(),
                    context::getFieldTypeOrNull,
                    context.queryShardContext);
            }

            @Override
            protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, Context context) {
                // col < ANY ([1,2,3]) --> or(col<1, col<2, col<3)
                BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
                booleanQuery.setMinimumNumberShouldMatch(1);
                for (Object value : toIterable(arrayLiteral.value())) {
                    booleanQuery.add(
                        inverseRangeQuery.toQuery(reference, value, context::getFieldTypeOrNull, context.queryShardContext),
                        BooleanClause.Occur.SHOULD);
                }
                return booleanQuery.build();
            }
        }

        static class RangeQuery extends CmpQuery {

            private final boolean includeLower;
            private final boolean includeUpper;
            private final java.util.function.Function<Object, Tuple<?, ?>> boundsFunction;

            private static final java.util.function.Function<Object, Tuple<?, ?>> LOWER_BOUND = in -> new Tuple<>(in, null);
            private static final java.util.function.Function<Object, Tuple<?, ?>> UPPER_BOUND = in -> new Tuple<>(null, in);

            RangeQuery(String comparison) {
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
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> tuple = super.prepare(input);
                if (tuple == null) {
                    return null;
                }
                return toQuery(tuple.v1(), tuple.v2().value(), context::getFieldTypeOrNull, context.queryShardContext);
            }

            public Query toQuery(Reference reference, Object value, FieldTypeLookup fieldTypeLookup, QueryShardContext queryShardContext) {
                String columnName = reference.ident().columnIdent().fqn();
                MappedFieldType fieldType = fieldTypeLookup.get(columnName);
                if (fieldType == null) {
                    // can't match column that doesn't exist or is an object ( "o >= {x=10}" is not supported)
                    return Queries.newMatchNoDocsQuery("column does not exist in this index");
                }
                Tuple<?, ?> bounds = boundsFunction.apply(value);
                assert bounds != null : "bounds must not be null";
                return fieldType.rangeQuery(bounds.v1(), bounds.v2(), includeLower, includeUpper, queryShardContext);
            }
        }

        static class ToMatchQuery implements FunctionToQuery {

            @Override
            public Query apply(Function input, Context context) throws IOException {
                List<Symbol> arguments = input.arguments();
                assert arguments.size() == 4 : "invalid number of arguments";
                assert Symbol.isLiteral(arguments.get(0), DataTypes.OBJECT) :
                    "fields must be literal";
                assert Symbol.isLiteral(arguments.get(2), DataTypes.STRING) :
                    "matchType must be literal";

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
                MappedFieldType fieldType = context.mapperService.fullName(fieldName);
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
                    default:
                        throw invalidMatchType(relation.getRelationName());
                }
            }

            private AssertionError invalidMatchType(String matchType) {
                throw new AssertionError(String.format(Locale.ENGLISH,
                    "Invalid match type: %s. Analyzer should have made sure that it is valid", matchType));
            }

            private static Query stringMatch(Context context, List<Symbol> arguments, Object queryTerm) throws IOException {
                @SuppressWarnings("unchecked")
                Map<String, Object> fields = (Map) ((Literal) arguments.get(0)).value();
                BytesRef queryString = (BytesRef) queryTerm;
                BytesRef matchType = (BytesRef) ((Literal) arguments.get(2)).value();
                //noinspection unchecked
                Map<String, Object> options = (Map<String, Object>) ((Literal) arguments.get(3)).value();

                MatchOptionsAnalysis.validate(options);

                if (fields.size() == 1) {
                    return singleMatchQuery(
                        context.queryShardContext,
                        fields.entrySet().iterator().next(),
                        queryString,
                        matchType,
                        options
                    );
                } else {
                    fields.replaceAll((s, o) -> {
                        if (o instanceof Number) {
                            return ((Number) o).floatValue();
                        }
                        return null;
                    });
                    //noinspection unchecked
                    return MatchQueries.multiMatch(
                        context.queryShardContext,
                        matchType,
                        (Map<String, Float>) (Map) fields,
                        queryString.utf8ToString(),
                        options
                    );
                }
            }

            private static Query singleMatchQuery(QueryShardContext queryShardContext,
                                                  Map.Entry<String, Object> entry,
                                                  BytesRef queryString,
                                                  BytesRef matchType,
                                                  Map<String, Object> options) throws IOException {
                Query query = MatchQueries.singleMatch(
                    queryShardContext,
                    entry.getKey(),
                    queryString,
                    matchType,
                    options
                );
                Object boost = entry.getValue();
                if (boost instanceof Number) {
                    return new BoostQuery(query, ((Number) boost).floatValue());
                }
                return query;
            }
        }

        static class RegexpMatchQuery extends CmpQuery {

            private Query toLuceneRegexpQuery(String fieldName, BytesRef value) {
                return new ConstantScoreQuery(
                    new RegexpQuery(new Term(fieldName, value), RegExp.ALL));
            }

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> prepare = prepare(input);
                if (prepare == null) {
                    return null;
                }
                String fieldName = prepare.v1().ident().columnIdent().fqn();
                BytesRef pattern = BytesRefs.toBytesRef(prepare.v2().value());
                if (pattern == null) {
                    // cannot build query using null pattern value
                    return null;
                }

                if (isPcrePattern(pattern.utf8ToString())) {
                    return new CrateRegexQuery(new Term(fieldName, pattern));
                } else {
                    return toLuceneRegexpQuery(fieldName, pattern);
                }
            }
        }

        static class RegexMatchQueryCaseInsensitive extends CmpQuery {

            @Override
            public Query apply(Function input, Context context) {
                Tuple<Reference, Literal> prepare = prepare(input);
                if (prepare == null) {
                    return null;
                }
                String fieldName = prepare.v1().ident().columnIdent().fqn();
                Object value = prepare.v2().value();

                if (value instanceof BytesRef) {
                    return new CrateRegexQuery(
                        new Term(fieldName, BytesRefs.toBytesRef(value)),
                        CrateRegexCapabilities.FLAG_CASE_INSENSITIVE | CrateRegexCapabilities.FLAG_UNICODE_CASE);
                }
                throw new IllegalArgumentException("Can only use ~* with patterns of type string");
            }
        }

        /**
         * interface for functions that can be used to generate a query from inner functions.
         * Has only a single method {@link #apply(io.crate.expression.symbol.Function, io.crate.expression.symbol.Function, io.crate.lucene.LuceneQueryBuilder.Context)}
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
            public Query apply(Function parent, Function inner, Context context) {
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
                Geometry geometry;
                Shape shape = GeoJSONUtils.map2Shape(geoJSON);
                if (shape instanceof ShapeCollection) {
                    int i = 0;
                    ShapeCollection<Shape> collection = (ShapeCollection) shape;
                    com.vividsolutions.jts.geom.Polygon[] polygons = new com.vividsolutions.jts.geom.Polygon[collection.size()];
                    for (Shape s : collection.getShapes()) {
                        Geometry subGeometry = JtsSpatialContext.GEO.getShapeFactory().getGeometryFrom(s);
                        if (subGeometry instanceof com.vividsolutions.jts.geom.Polygon) {
                            polygons[i++] = (com.vividsolutions.jts.geom.Polygon) subGeometry;
                        } else {
                            throw new InvalidShapeException("Shape collection must contain only Polygon shapes.");
                        }
                    }
                    GeometryFactory geometryFactory = JtsSpatialContext.GEO.getShapeFactory().getGeometryFactory();
                    geometry = geometryFactory.createMultiPolygon(polygons);
                } else {
                    geometry = JtsSpatialContext.GEO.getShapeFactory().getGeometryFrom(shape);
                }

                IndexGeoPointFieldData fieldData = context.fieldDataService.getForField(geoPointFieldType);
                if (geometry.isRectangle()) {
                    return getBoundingBoxQuery(shape, fieldData);
                } else {
                    final Version indexVersionCreated = context.queryShardContext.indexVersionCreated();
                    return getPolygonQuery(geometry, fieldData, indexVersionCreated, geoPointFieldType);
                }
            }

            private static Query getPolygonQuery(Geometry geometry,
                                                 IndexGeoPointFieldData fieldData,
                                                 Version indexVersionCreated,
                                                 BaseGeoPointFieldMapper.GeoPointFieldType fieldType) {
                Coordinate[] coordinates = geometry.getCoordinates();
                // close the polygon shape if startpoint != endpoint
                if (!CoordinateArrays.isRing(coordinates)) {
                    coordinates = Arrays.copyOf(coordinates, coordinates.length + 1);
                    coordinates[coordinates.length - 1] = coordinates[0];
                }

                if (indexVersionCreated.before(Version.V_2_2_0)) {
                    GeoPoint[] points = new GeoPoint[coordinates.length];
                    for (int i = 0; i < coordinates.length; i++) {
                        points[i] = new GeoPoint(coordinates[i].y, coordinates[i].x);
                    }
                    return new GeoPolygonQuery(fieldData, points);
                }

                final double[] lats = new double[coordinates.length];
                final double[] lons = new double[coordinates.length];
                for (int i = 0; i < coordinates.length; i++) {
                    lats[i] = coordinates[i].y;
                    lons[i] = coordinates[i].x;
                }

                if (indexVersionCreated.onOrAfter(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
                    return LatLonPoint.newPolygonQuery(fieldType.name(), new Polygon(lats, lons));
                }

                // if index created V_2_2 use (soon to be legacy) numeric encoding postings format
                // if index created V_2_3 > use prefix encoded postings format
                final GeoPointField.TermEncoding encoding = indexVersionCreated.before(Version.V_2_3_0) ?
                    GeoPointField.TermEncoding.NUMERIC : GeoPointField.TermEncoding.PREFIX;
                return new GeoPointInPolygonQuery(
                    fieldData.getFieldName(),
                    encoding,
                    new Polygon(lats, lons)
                );
            }

            private Query getBoundingBoxQuery(Shape shape, IndexGeoPointFieldData fieldData) {
                Rectangle boundingBox = shape.getBoundingBox();
                return new LegacyInMemoryGeoBoundingBoxQuery(
                    new GeoPoint(boundingBox.getMaxY(), boundingBox.getMinX()),
                    new GeoPoint(boundingBox.getMinY(), boundingBox.getMaxX()),
                    fieldData
                );
            }

            @Override
            public Query apply(Function input, Context context) {
                return getQuery(input, context);
            }
        }

        static class DistanceQuery implements InnerFunctionToQuery {

            /**
             * @param parent the outer function. E.g. in the case of
             *               <pre>where distance(p1, POINT (10 20)) > 20</pre>
             *               this would be
             *               <pre>gt( \<inner function\>,  20)</pre>
             * @param inner  has to be the distance function
             */
            @Override
            public Query apply(Function parent, Function inner, Context context) {
                assert inner.info().ident().name().equals(DistanceFunction.NAME) :
                    "function must be " + DistanceFunction.NAME;

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


                Version indexVersionCreated = context.indexCache.getIndexSettings().getIndexVersionCreated();

                String parentName = functionLiteralPair.functionName();
                Input geoPointInput = distanceRefLiteral.input();
                String fieldName = distanceRefLiteral.reference().ident().columnIdent().fqn();
                Double[] pointValue = (Double[]) geoPointInput.value();
                if (indexVersionCreated.onOrAfter(LatLonPointFieldMapper.LAT_LON_FIELD_VERSION)) {
                    return esV5DistanceQuery(parent, context, parentName, fieldName, distance, pointValue);
                }

                double lat = pointValue[1];
                double lon = pointValue[0];

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
                GeoUtils.normalizePoint(geoPoint);
                if (from == null) {
                    from = 0d;
                }
                if (to == null) {
                    to = GeoUtils.maxRadialDistanceMeters(geoPoint.lat(), geoPoint.lon());
                }
                BaseGeoPointFieldMapper.GeoPointFieldType geoPointFieldType = getGeoPointFieldType(
                    fieldName, context.mapperService);
                IndexGeoPointFieldData fieldData = context.fieldDataService.getForField(geoPointFieldType);
                return new XGeoPointDistanceRangeQuery(
                    fieldData.index().getName(),
                    GeoPointField.TermEncoding.PREFIX,
                    geoPoint.lat(),
                    geoPoint.lon(),
                    (includeLower) ? from : from + TOLERANCE,
                    (includeUpper) ? to : to - TOLERANCE);
            }
        }

        private static BaseGeoPointFieldMapper.GeoPointFieldType getGeoPointFieldType(String fieldName, MapperService mapperService) {
            MappedFieldType fieldType = mapperService.fullName(fieldName);
            if (fieldType == null) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "column \"%s\" doesn't exist", fieldName));
            }
            if (!(fieldType instanceof GeoPointFieldMapper.GeoPointFieldType)) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "column \"%s\" isn't of type geo_point", fieldName));
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
                .put(NotPredicate.NAME, new NotQuery())
                .put(IsNullPredicate.NAME, new IsNullQuery())
                .put(MatchPredicate.NAME, new ToMatchQuery())
                .put(AnyOperators.Names.EQ, new AnyEqQuery())
                .put(AnyOperators.Names.NEQ, new AnyNeqQuery())
                .put(AnyOperators.Names.LT, new AnyRangeQuery("gt", "lt"))
                .put(AnyOperators.Names.LTE, new AnyRangeQuery("gte", "lte"))
                .put(AnyOperators.Names.GTE, new AnyRangeQuery("lte", "gte"))
                .put(AnyOperators.Names.GT, new AnyRangeQuery("lt", "gt"))
                .put(AnyLikeOperator.LIKE, new AnyLikeQuery())
                .put(AnyLikeOperator.NOT_LIKE, new AnyNotLikeQuery())
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
            assert function != null : "function must not be null";
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

        private Function rewriteAndValidateFields(Function function, Context context) {
            if (function.arguments().size() == 2) {
                Symbol left = function.arguments().get(0);
                Symbol right = function.arguments().get(1);
                if (left.symbolType() == SymbolType.REFERENCE && right.symbolType().isValueSymbol()) {
                    Reference ref = (Reference) left;
                    if (ref.ident().columnIdent().equals(DocSysColumns.ID)) {
                        Literal uidLiteral = generateLuceneUIDs(right);
                        return new Function(
                            function.info(),
                            Arrays.asList(
                                DocSysColumns.forTable(ref.ident().tableIdent(), DocSysColumns.UID),
                                uidLiteral));
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

        /**
         * Converts the Symbol value(s) to Lucene string(s) to match against the Lucene ids.
         * @param symbol The Symbol to convert to a String uid.
         * @return A literal holding the Lucene query value string.
         */
        private static Literal generateLuceneUIDs(Symbol symbol) {
            if (DataTypes.isCollectionType(symbol.valueType())) {
                List<String> strings = ValueSymbolVisitor.STRING_LIST.process(symbol);
                String[] uids = new String[strings.size()];
                for (int i = 0; i < uids.length; i++) {
                    uids[i] = Uid.createUid(Constants.DEFAULT_MAPPING_TYPE, strings.get(i));
                }
                return Literal.of(new ArrayType(DataTypes.STRING), uids);
            } else {
                String uid = Uid.createUid(Constants.DEFAULT_MAPPING_TYPE, ValueSymbolVisitor.STRING.process(symbol));
                return Literal.of(uid);
            }
        }

        static Query genericFunctionFilter(Function function, Context context) {
            if (function.valueType() != DataTypes.BOOLEAN) {
                raiseUnsupported(function);
            }
            // rewrite references to source lookup instead of using the docValues column store if:
            // - no docValues are available for the related column, currently only on objects defined as `ignored`
            // - docValues value differs from source, currently happening on GeoPoint types as lucene's internal format
            //   results in precision changes (e.g. longitude 11.0 will be 10.999999966)
            function = (Function) DocReferences.toSourceLookup(function,
                r -> r.columnPolicy() == ColumnPolicy.IGNORED
                     || r.valueType() == DataTypes.GEO_POINT);

            final InputFactory.Context<? extends LuceneCollectorExpression<?>> ctx = context.docInputFactory.getCtx();
            @SuppressWarnings("unchecked")
            final Input<Boolean> condition = (Input<Boolean>) ctx.add(function);
            @SuppressWarnings("unchecked")
            final Collection<? extends LuceneCollectorExpression<?>> expressions = ctx.expressions();
            final CollectorContext collectorContext = new CollectorContext(
                context.fieldDataService,
                new CollectorFieldsVisitor(expressions.size())
            );

            for (LuceneCollectorExpression expression : expressions) {
                expression.startCollect(collectorContext);
            }
            return new GenericFunctionQuery(function, expressions, collectorContext, condition);
        }

        private static void raiseUnsupported(Function function) {
            throw new UnsupportedOperationException(
                SymbolFormatter.format("Cannot convert function %s into a query", function));
        }

        @Override
        public Query visitReference(Reference symbol, Context context) {
            // called for queries like: where boolColumn
            if (symbol.valueType() == DataTypes.BOOLEAN) {
                MappedFieldType fieldType = context.getFieldTypeOrNull(symbol.ident().columnIdent().fqn());
                if (fieldType == null) {
                    return Queries.newMatchNoDocsQuery("column does not exist in this index");
                }
                return fieldType.termQuery(true, null);
            }
            return super.visitReference(symbol, context);
        }

        @Override
        protected Query visitSymbol(Symbol symbol, Context context) {
            throw new UnsupportedOperationException(
                SymbolFormatter.format("Can't build query from symbol %s", symbol));
        }
    }

    private static class FunctionLiteralPair {

        private final String functionName;
        private final Function function;
        private final Input input;

        private FunctionLiteralPair(Function outerFunction) {
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

        private boolean isValid() {
            return input != null && function != null;
        }

        private Input input() {
            return input;
        }

        private String functionName() {
            return functionName;
        }
    }

    private static class RefLiteralPair {

        private final Reference reference;
        private final Input input;

        private RefLiteralPair(Function function) {
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

        private boolean isValid() {
            return input != null && reference != null;
        }

        private Reference reference() {
            return reference;
        }

        private Input input() {
            return input;
        }
    }
}
