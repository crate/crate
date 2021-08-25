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

import java.io.IOException;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import io.crate.expression.operator.LikeOperators.CaseSensitivity;
import io.crate.expression.operator.any.AnyOperators;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.Reference;

class AnyLikeQuery extends AbstractAnyQuery {

    AnyLikeQuery(CaseSensitivity caseSensitivity) {
        super(caseSensitivity);
    }

    @Override
    protected Query literalMatchesAnyArrayRef(Literal candidate, Reference array, LuceneQueryBuilder.Context context) throws IOException {
        return LikeQuery.toQuery(array, candidate.value(), context, caseSensitivity);
    }

    @Override
    protected Query refMatchesAnyArrayLiteral(Reference candidate, Literal array, LuceneQueryBuilder.Context context) {
        // col like ANY (['a', 'b']) --> or(like(col, 'a'), like(col, 'b'))
        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
        booleanQuery.setMinimumNumberShouldMatch(1);
        for (Object value : AnyOperators.collectionValueToIterable(array.value())) {
            booleanQuery.add(LikeQuery.toQuery(candidate, value, context, caseSensitivity), BooleanClause.Occur.SHOULD);
        }
        return booleanQuery.build();
    }
}
