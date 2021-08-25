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
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.expression.operator.LikeOperators.CaseSensitivity;
import io.crate.expression.operator.any.AnyOperators;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.types.DataTypes;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nullable;
import java.io.IOException;

abstract class AbstractAnyQuery implements FunctionToQuery {

    protected final CaseSensitivity caseSensitivity;

    AbstractAnyQuery() {
        this(CaseSensitivity.SENSITIVE);
    }

    AbstractAnyQuery(CaseSensitivity caseSensitivity) {
        this.caseSensitivity = caseSensitivity;
    }

    @Override
    public Query apply(Function function, LuceneQueryBuilder.Context context) throws IOException {
        Symbol left = function.arguments().get(0);
        Symbol collectionSymbol = function.arguments().get(1);
        if (!DataTypes.isArray(collectionSymbol.valueType())) {
            throw new IllegalArgumentException("invalid argument for ANY expression");
        }

        if (DataTypes.isArray(left.valueType())) {
            throw new UnsupportedFeatureException(
                "Cannot use " + function.name() + " when the left side is an array");
        }
        if (left.symbolType().isValueSymbol()) {
            // 1 = any (array_col) - simple eq
            if (collectionSymbol instanceof Reference) {
                return literalMatchesAnyArrayRef((Literal<?>) left, (Reference) collectionSymbol, context);
            } else {
                // no reference found (maybe subscript) in ANY expression -> fallback to slow generic function filter
                return null;
            }
        } else if (left instanceof Reference && collectionSymbol.symbolType().isValueSymbol()) {
            return refMatchesAnyArrayLiteral((Reference) left, (Literal<?>) collectionSymbol, context);
        } else {
            // might be the case if the left side is a function -> will fallback to (slow) generic function filter
            return null;
        }
    }

    /**
     * converts Strings to BytesRef on the fly
     */
    static Iterable<?> toIterable(Object value) {
        return Iterables.transform(AnyOperators.collectionValueToIterable(value), new com.google.common.base.Function<Object, Object>() {
            @Nullable
            @Override
            public Object apply(@Nullable Object input) {
                if (input instanceof String) {
                    input = new BytesRef((String) input);
                }
                return input;
            }
        });
    }

    /**
     * Generate a query for:
     * <pre>
     * {@code
     *  <candidate> <OP> ANY (array)
     * }
     * </pre>
     * <p>
     * Where candidate is a literal and array a reference
     */
    protected abstract Query literalMatchesAnyArrayRef(Literal<?> candidate,
                                                       Reference array,
                                                       LuceneQueryBuilder.Context context) throws IOException;

    /**
     * Generate a query for:
     * <pre>
     * {@code
     *  <candidate> <OP> ANY (array)
     * }
     * </pre>
     * <p>
     * Where candidate is a reference and array a literal
     */
    protected abstract Query refMatchesAnyArrayLiteral(Reference candidate,
                                                       Literal<?> array,
                                                       LuceneQueryBuilder.Context context) throws IOException;
}
