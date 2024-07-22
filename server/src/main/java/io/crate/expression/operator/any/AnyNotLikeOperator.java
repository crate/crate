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

package io.crate.expression.operator.any;

import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.query.RegexpFlag;
import org.jetbrains.annotations.NotNull;

import io.crate.expression.operator.LikeOperators;
import io.crate.expression.operator.LikeOperators.CaseSensitivity;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;

public final class AnyNotLikeOperator extends AnyOperator<String> {

    private final CaseSensitivity caseSensitivity;

    public AnyNotLikeOperator(Signature signature,
                       BoundSignature boundSignature,
                       CaseSensitivity caseSensitivity) {
        super(signature, boundSignature);
        this.caseSensitivity = caseSensitivity;
    }

    private static String negateWildcard(String wildCard) {
        return "~(" + wildCard + ')';
    }

    @Override
    boolean matches(String probe, String candidate) {
        // Accept both sides of arguments to be patterns
        return !LikeOperators.matches(probe, candidate, LikeOperators.DEFAULT_ESCAPE, caseSensitivity) &&
               !LikeOperators.matches(candidate, probe, LikeOperators.DEFAULT_ESCAPE, caseSensitivity);
    }

    @Override
    protected Query refMatchesAnyArrayLiteral(Function any, Reference probe, @NotNull List<?> nonNullValues, Context context) {
        // col not like ANY (['a', 'b']) --> not(and(like(col, 'a'), like(col, 'b')))
        String columnName = probe.storageIdent();
        BooleanQuery.Builder andLikeQueries = new BooleanQuery.Builder();
        for (Object value : nonNullValues) {
            var likeQuery = caseSensitivity.likeQuery(columnName,
                (String) value,
                LikeOperators.DEFAULT_ESCAPE,
                probe.indexType() != IndexType.NONE
            );
            if (likeQuery == null) {
                return null;
            }
            andLikeQueries.add(likeQuery, BooleanClause.Occur.MUST);
        }
        return Queries.not(andLikeQueries.build());
    }

    @Override
    protected Query literalMatchesAnyArrayRef(Function any, Literal<?> probe, Reference candidates, Context context) {
        String pattern = (String) probe.value();
        String regexString = LikeOperators.patternToRegex(pattern, LikeOperators.DEFAULT_ESCAPE);
        regexString = regexString.substring(1, regexString.length() - 1);
        String notLike = negateWildcard(regexString);

        return new RegexpQuery(new Term(
            candidates.storageIdent(),
            notLike),
            RegexpFlag.COMPLEMENT.value()
        );
    }

    @Override
    protected void validateRightArg(String arg) {
        if (arg.endsWith(LikeOperators.DEFAULT_ESCAPE_STR)) {
            LikeOperators.throwErrorForTrailingEscapeChar(arg, LikeOperators.DEFAULT_ESCAPE);
        }
    }
}
