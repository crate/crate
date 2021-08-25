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
import java.util.Locale;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.RegexpFlag;

import io.crate.expression.operator.LikeOperators;
import io.crate.expression.operator.LikeOperators.CaseSensitivity;
import io.crate.expression.operator.any.AnyOperators;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.Reference;

class AnyNotLikeQuery extends AbstractAnyQuery {

    private static String negateWildcard(String wildCard) {
        return String.format(Locale.ENGLISH, "~(%s)", wildCard);
    }

    AnyNotLikeQuery(CaseSensitivity caseSensitivity) {
        super(caseSensitivity);
    }

    @Override
    protected Query literalMatchesAnyArrayRef(Literal candidate, Reference array, LuceneQueryBuilder.Context context) throws IOException {
        String regexString = LikeOperators.patternToRegex((String) candidate.value(), LikeOperators.DEFAULT_ESCAPE, false);
        regexString = regexString.substring(1, regexString.length() - 1);
        String notLike = negateWildcard(regexString);

        return new RegexpQuery(new Term(
            array.column().fqn(),
            notLike),
            RegexpFlag.COMPLEMENT.value()
        );
    }

    @Override
    protected Query refMatchesAnyArrayLiteral(Reference candidate, Literal array, LuceneQueryBuilder.Context context) {
        // col not like ANY (['a', 'b']) --> not(and(like(col, 'a'), like(col, 'b')))
        String columnName = candidate.column().fqn();
        MappedFieldType fieldType = context.getFieldTypeOrNull(columnName);

        BooleanQuery.Builder andLikeQueries = new BooleanQuery.Builder();
        for (Object value : AnyOperators.collectionValueToIterable(array.value())) {
            andLikeQueries.add(
                LikeQuery.like(candidate.valueType(), fieldType, value, caseSensitivity),
                BooleanClause.Occur.MUST);
        }
        return Queries.not(andLikeQueries.build());
    }
}
