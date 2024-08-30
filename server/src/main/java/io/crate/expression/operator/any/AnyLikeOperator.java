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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;

import io.crate.expression.operator.LikeOperators;
import io.crate.expression.operator.LikeOperators.CaseSensitivity;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;


public final class AnyLikeOperator extends AnyOperator<String> {

    private final CaseSensitivity caseSensitivity;

    public AnyLikeOperator(Signature signature,
                           BoundSignature boundSignature,
                           CaseSensitivity caseSensitivity) {
        super(signature, boundSignature);
        this.caseSensitivity = caseSensitivity;
        DataType<?> innerType = ((ArrayType<?>) boundSignature.argTypes().get(1)).innerType();
        if (innerType.id() == ObjectType.ID) {
            throw new IllegalArgumentException("ANY on object arrays is not supported");
        }
    }

    @Override
    boolean matches(String probe, String candidate) {
        // Accept both sides of arguments to be patterns
        return LikeOperators.matches(probe, candidate, LikeOperators.DEFAULT_ESCAPE, caseSensitivity) ||
               LikeOperators.matches(candidate, probe, LikeOperators.DEFAULT_ESCAPE, caseSensitivity);
    }

    @Override
    protected Query refMatchesAnyArrayLiteral(Function any, Reference probe, Literal<?> candidates, Context context) {
        if (ArrayType.dimensions(candidates.valueType()) > 1) {
            return null;
        }
        var nonNullValues = filterNullValues(candidates);
        if (nonNullValues.isEmpty()) {
            return new MatchNoDocsQuery("Cannot match unless there is at least one non-null candidate");
        }
        // col like ANY (['a', 'b']) --> or(like(col, 'a'), like(col, 'b'))
        String fqn = probe.storageIdent();
        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
        booleanQuery.setMinimumNumberShouldMatch(1);
        for (Object value : nonNullValues) {
            var likeQuery = caseSensitivity.likeQuery(fqn, (String) value, LikeOperators.DEFAULT_ESCAPE, probe.indexType() != IndexType.NONE);
            if (likeQuery == null) {
                return null;
            }
            booleanQuery.add(likeQuery, BooleanClause.Occur.SHOULD);
        }
        return booleanQuery.build();
    }

    @Override
    protected Query literalMatchesAnyArrayRef(Function any, Literal<?> probe, Reference candidates, Context context) {
        return caseSensitivity.likeQuery(candidates.storageIdent(), (String) probe.value(), LikeOperators.DEFAULT_ESCAPE, candidates.indexType() != IndexType.NONE);
    }

    @Override
    protected void validateRightArg(String arg) {
        if (arg.endsWith(LikeOperators.DEFAULT_ESCAPE_STR)) {
            LikeOperators.throwErrorForTrailingEscapeChar(arg, LikeOperators.DEFAULT_ESCAPE);
        }
    }
}
