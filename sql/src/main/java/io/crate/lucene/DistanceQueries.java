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

package io.crate.lucene;

import io.crate.analyze.symbol.Function;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.GtOperator;
import io.crate.operation.operator.GteOperator;
import io.crate.operation.operator.LtOperator;
import io.crate.operation.operator.LteOperator;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.lucene.search.Queries;

import static io.crate.lucene.LuceneQueryBuilder.Visitor.genericFunctionFilter;

enum DistanceQueries {
    ;

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
    public static Query esV5DistanceQuery(Function parentFunction,
                                          LuceneQueryBuilder.Context context,
                                          String parentOperatorName,
                                          String columnName,
                                          Double distance,
                                          Double[] lonLat) {
        switch (parentOperatorName) {
            // We documented that using distance in the WHERE clause utilizes the index which isn't precise so treating
            // lte & lt the same should be acceptable
            case LteOperator.NAME:
            case LtOperator.NAME:
                return LatLonPoint.newDistanceQuery(columnName, lonLat[1], lonLat[0], distance);
            case GteOperator.NAME:
                if (distance - GeoUtils.TOLERANCE <= 0.0d) {
                    return Queries.newMatchAllQuery();
                }
                // fall through
            case GtOperator.NAME:
                return Queries.not(LatLonPoint.newDistanceQuery(columnName, lonLat[1], lonLat[0], distance));
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
                                    Double[] lonLat) {
        double smallDistance = distance * 0.99;
        if (smallDistance <= 0.0) {
            return LatLonPoint.newDistanceQuery(columnName, lonLat[1], lonLat[0], 0);
        }
        Query withinSmallCircle = LatLonPoint.newDistanceQuery(columnName, lonLat[1], lonLat[0], smallDistance);
        Query withinLargeCircle = LatLonPoint.newDistanceQuery(columnName, lonLat[1], lonLat[0], distance * 1.01);
        return new BooleanQuery.Builder()
            .add(withinLargeCircle, BooleanClause.Occur.MUST)
            .add(withinSmallCircle, BooleanClause.Occur.MUST_NOT)
            .add(genericFunctionFilter(parentFunction, context), BooleanClause.Occur.FILTER)
            .build();
    }
}
