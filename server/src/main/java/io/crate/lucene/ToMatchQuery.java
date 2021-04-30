/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import com.google.common.collect.Iterables;
import io.crate.analyze.MatchOptionsAnalysis;
import io.crate.data.Input;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.geo.GeoJSONUtils;
import io.crate.lucene.match.MatchQueries;
import io.crate.types.DataTypes;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.locationtech.spatial4j.shape.Shape;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

class ToMatchQuery implements FunctionToQuery {

    @Override
    public Query apply(Function input, LuceneQueryBuilder.Context context) throws IOException {
        List<Symbol> arguments = input.arguments();
        assert arguments.size() == 4 : "invalid number of arguments";
        assert Symbol.isLiteral(arguments.get(0), DataTypes.UNTYPED_OBJECT) :
            "fields must be literal";
        assert Symbol.isLiteral(arguments.get(2), DataTypes.STRING) :
            "matchType must be literal";

        Symbol queryTerm = arguments.get(1);
        if (!(queryTerm instanceof Input)) {
            throw new IllegalArgumentException("queryTerm must be a literal");
        }
        Object queryTermVal = ((Input) queryTerm).value();
        if (queryTermVal == null) {
            throw new IllegalArgumentException("cannot use NULL as query term in match predicate");
        }
        if (queryTerm.valueType().equals(DataTypes.GEO_SHAPE)) {
            return geoMatch(context, arguments, queryTermVal);
        }
        return stringMatch(context, arguments, queryTermVal);
    }

    private Query geoMatch(LuceneQueryBuilder.Context context, List<Symbol> arguments, Object queryTerm) {

        Map fields = (Map) ((Literal) arguments.get(0)).value();
        String fieldName = ((String) Iterables.getOnlyElement(fields.keySet()));
        MappedFieldType fieldType = context.mapperService.fullName(fieldName);
        GeoShapeFieldMapper.GeoShapeFieldType geoShapeFieldType = (GeoShapeFieldMapper.GeoShapeFieldType) fieldType;
        String matchType = (String) ((Input) arguments.get(2)).value();
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

    private static Query stringMatch(LuceneQueryBuilder.Context context, List<Symbol> arguments, Object queryTerm) throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, Object> fields = (Map) ((Literal) arguments.get(0)).value();
        String queryString = (String) queryTerm;
        String matchType = (String) ((Literal) arguments.get(2)).value();
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
                queryString,
                options
            );
        }
    }

    private static Query singleMatchQuery(QueryShardContext queryShardContext,
                                          Map.Entry<String, Object> entry,
                                          String queryString,
                                          String matchType,
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
