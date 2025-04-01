/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.expression.operator.all;

import org.apache.lucene.search.Query;

import io.crate.expression.operator.LikeOperators;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.Reference;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;

public final class AllLikeOperator extends AllOperator<String> {

    private final LikeOperators.CaseSensitivity caseSensitivity;

    public AllLikeOperator(Signature signature,
                           BoundSignature boundSignature,
                           LikeOperators.CaseSensitivity caseSensitivity) {
        super(signature, boundSignature);
        this.caseSensitivity = caseSensitivity;
        DataType<?> innerType = ((ArrayType<?>) boundSignature.argTypes().get(1)).innerType();
        if (innerType.id() == ObjectType.ID) {
            throw new IllegalArgumentException("ALL on object arrays is not supported");
        }
    }

    @Override
    boolean matches(String probe, String candidate) {
        return LikeOperators.matches(probe, candidate, LikeOperators.DEFAULT_ESCAPE, caseSensitivity);
    }

    @Override
    protected Query refMatchesAllArrayLiteral(Function all, Reference probe, Literal<?> literal, LuceneQueryBuilder.Context context) {
        return null;
    }

    @Override
    protected Query literalMatchesAllArrayRef(Function all, Literal<?> probe, Reference candidates, LuceneQueryBuilder.Context context) {
        return null;
    }

    @Override
    protected void validateRightArg(String arg) {
        if (arg.endsWith(LikeOperators.DEFAULT_ESCAPE_STR)) {
            LikeOperators.throwErrorForTrailingEscapeChar(arg, LikeOperators.DEFAULT_ESCAPE);
        }
    }
}
