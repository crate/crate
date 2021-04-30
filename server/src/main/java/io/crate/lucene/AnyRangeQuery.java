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

import io.crate.expression.symbol.Literal;
import io.crate.metadata.Reference;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import java.io.IOException;

class AnyRangeQuery extends AbstractAnyQuery {

    private final RangeQuery rangeQuery;
    private final RangeQuery inverseRangeQuery;

    AnyRangeQuery(String comparison, String inverseComparison) {
        rangeQuery = new RangeQuery(comparison);
        inverseRangeQuery = new RangeQuery(inverseComparison);
    }

    @Override
    protected Query literalMatchesAnyArrayRef(Literal candidate, Reference array, LuceneQueryBuilder.Context context) throws IOException {
        // 1 < ANY (array_col) --> array_col > 1
        return rangeQuery.toQuery(
            array,
            candidate.value(),
            context::getFieldTypeOrNull,
            context.queryShardContext);
    }

    @Override
    protected Query refMatchesAnyArrayLiteral(Reference candidate, Literal array, LuceneQueryBuilder.Context context) {
        // col < ANY ([1,2,3]) --> or(col<1, col<2, col<3)
        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
        booleanQuery.setMinimumNumberShouldMatch(1);
        for (Object value : toIterable(array.value())) {
            booleanQuery.add(
                inverseRangeQuery.toQuery(candidate, value, context::getFieldTypeOrNull, context.queryShardContext),
                BooleanClause.Occur.SHOULD);
        }
        return booleanQuery.build();
    }
}
