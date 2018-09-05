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

import io.crate.data.Input;
import io.crate.expression.scalar.geo.DistanceFunction;
import io.crate.expression.symbol.Function;
import io.crate.types.DataTypes;
import org.apache.lucene.search.Query;

import static io.crate.lucene.DistanceQueries.esV5DistanceQuery;

class DistanceQuery implements InnerFunctionToQuery {

    /**
     * @param parent the outer function. E.g. in the case of
     *               <pre>where distance(p1, POINT (10 20)) > 20</pre>
     *               this would be
     *               <pre>gt( \<inner function\>,  20)</pre>
     * @param inner  has to be the distance function
     */
    @Override
    public Query apply(Function parent, Function inner, LuceneQueryBuilder.Context context) {
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

        String parentName = functionLiteralPair.functionName();
        Input geoPointInput = distanceRefLiteral.input();
        String fieldName = distanceRefLiteral.reference().column().fqn();
        Double[] pointValue = (Double[]) geoPointInput.value();
        return esV5DistanceQuery(parent, context, parentName, fieldName, distance, pointValue);
    }
}
