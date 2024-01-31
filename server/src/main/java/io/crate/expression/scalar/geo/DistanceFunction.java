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

package io.crate.expression.scalar.geo;

import static io.crate.metadata.functions.Signature.scalar;

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.lucene.search.Queries;
import org.locationtech.spatial4j.shape.Point;

import io.crate.data.Input;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.GtOperator;
import io.crate.expression.operator.GteOperator;
import io.crate.expression.operator.LtOperator;
import io.crate.expression.operator.LteOperator;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public class DistanceFunction extends Scalar<Double, Point> {

    public static final String NAME = "distance";

    public static void register(ScalarFunctionModule module) {
        module.register(
            scalar(
                NAME,
                DataTypes.GEO_POINT.getTypeSignature(),
                DataTypes.GEO_POINT.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()
            ).withFeature(Feature.NULLABLE),
            DistanceFunction::new
        );
    }

    private DistanceFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public Double evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Point>[] args) {
        assert args.length == 2 : "number of args must be 2";
        return evaluate(args[0], args[1]);
    }

    public static Double evaluate(Input<Point> arg1, Input<Point> arg2) {
        Point value1 = arg1.value();
        if (value1 == null) {
            return null;
        }
        Point value2 = arg2.value();
        if (value2 == null) {
            return null;
        }
        return GeoUtils.arcDistance(value1.getY(), value1.getX(), value2.getY(), value2.getX());
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx, NodeContext nodeCtx) {
        Symbol arg1 = symbol.arguments().get(0);
        Symbol arg2 = symbol.arguments().get(1);

        boolean arg1IsReference = true;
        short numLiterals = 0;

        if (arg1.symbolType().isValueSymbol()) {
            numLiterals++;
            arg1IsReference = false;
        }

        if (arg2.symbolType().isValueSymbol()) {
            numLiterals++;
        }

        if (numLiterals == 2) {
            return Literal.of(evaluate((Input) arg1, (Input) arg2));
        }

        // ensure reference is the first argument.
        if (!arg1IsReference) {
            return new Function(signature, Arrays.asList(arg2, arg1), signature.getReturnType().createType());
        }
        return symbol;
    }

    @Override
    public Query toQuery(Function parent, Function inner, Context context) {
        assert inner.name().equals(DistanceFunction.NAME) :
            "function must be " + DistanceFunction.NAME;

        //                 ┌─► pointLiteral
        //                 │
        // distance(p, 'POINT (1 2)') = 10
        //          │                   │
        //          └► pointRef         │
        // ^^^^^^^^^^^^^^^^^^^^^^^^^^   └► parentRhs
        //          inner
        //
        // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        //              parent
        //
        List<Symbol> innerArgs = inner.arguments();
        if (!(innerArgs.get(0) instanceof Reference pointRef && innerArgs.get(1) instanceof Literal<?> pointLiteral)) {
            // can't use distance filter without literal, fallback to genericFunction
            return null;
        }
        List<Symbol> parentArgs = parent.arguments();
        if (!(parentArgs.get(1) instanceof Literal<?> parentRhs)) {
            // must be something like cmp(distance(..), non-literal) - fallback to genericFunction
            return null;
        }
        Double distance = DataTypes.DOUBLE.implicitCast(parentRhs.value());
        String parentName = parent.name();
        Point pointValue = (Point) pointLiteral.value();
        String fieldName = pointRef.storageIdent();
        return esV5DistanceQuery(parent, context, parentName, fieldName, distance, pointValue);
    }

    /**
     * Create a LatLonPoint distance query.
     *
     *
     * <pre>
     *          , - ~ ~ ~ - ,
     *      , '               ' ,
     *    ,                       ,     X = lonLat coordinates
     *   ,                         ,
     *  ,              [distance]   ,
     *  ,            p<------------>X
     *  ,            ^              ,
     *   ,           |             ,
     *    ,      [columnName]     ,
     *      ,     point        , '
     *        ' - , _ _ _ ,  '
     *
     *  lt and lte -> match everything WITHIN distance
     *  gt and gte -> match everything OUTSIDE distance
     *
     *  eq distance ~ 0 -> match everything within distance + tolerance
     *
     *  eq distance > 0 -> build two circles, one slightly smaller, one slightly larger
     *                          distance must not be within the smaller distance but within the larger.
     * </pre>
     */
    private static Query esV5DistanceQuery(Function parentFunction,
                                           LuceneQueryBuilder.Context context,
                                           String parentOperatorName,
                                           String columnName,
                                           Double distance,
                                           Point lonLat) {
        switch (parentOperatorName) {
            // We documented that using distance in the WHERE clause utilizes the index which isn't precise so treating
            // lte & lt the same should be acceptable
            case LteOperator.NAME:
            case LtOperator.NAME:
                return LatLonPoint.newDistanceQuery(columnName, lonLat.getY(), lonLat.getX(), distance);
            case GteOperator.NAME:
                if (distance - GeoUtils.TOLERANCE <= 0.0d) {
                    return new MatchAllDocsQuery();
                }
                // fall through
            case GtOperator.NAME:
                return Queries.not(LatLonPoint.newDistanceQuery(columnName, lonLat.getY(), lonLat.getX(), distance));
            case EqOperator.NAME:
                return eqDistance(parentFunction, context, columnName, distance, lonLat);
            default:
                return null;
        }
    }

    private static Query eqDistance(Function parentFunction,
                                    LuceneQueryBuilder.Context context,
                                    String columnName,
                                    Double distance,
                                    Point lonLat) {
        double smallDistance = distance * 0.99;
        if (smallDistance <= 0.0) {
            return LatLonPoint.newDistanceQuery(columnName, lonLat.getY(), lonLat.getX(), 0);
        }
        Query withinSmallCircle = LatLonPoint.newDistanceQuery(columnName, lonLat.getY(), lonLat.getX(), smallDistance);
        Query withinLargeCircle = LatLonPoint.newDistanceQuery(columnName, lonLat.getY(), lonLat.getX(), distance * 1.01);
        return new BooleanQuery.Builder()
            .add(withinLargeCircle, BooleanClause.Occur.MUST)
            .add(withinSmallCircle, BooleanClause.Occur.MUST_NOT)
            .add(LuceneQueryBuilder.genericFunctionFilter(parentFunction, context), BooleanClause.Occur.FILTER)
            .build();
    }

}
