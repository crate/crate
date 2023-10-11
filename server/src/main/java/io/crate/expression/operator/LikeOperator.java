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

package io.crate.expression.operator;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.lucene.search.Query;

import io.crate.data.Input;
import io.crate.expression.operator.LikeOperators.CaseSensitivity;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.user.UserLookup;

public class LikeOperator extends Operator<String> {

    private final TriPredicate<String, String, CaseSensitivity> matcher;
    private final CaseSensitivity caseSensitivity;

    public LikeOperator(Signature signature,
                        BoundSignature boundSignature,
                        TriPredicate<String, String, CaseSensitivity> matcher,
                        CaseSensitivity caseSensitivity) {
        super(signature, boundSignature);
        this.matcher = matcher;
        this.caseSensitivity = caseSensitivity;
    }

    @Override
    public Scalar<Boolean, String> compile(List<Symbol> arguments, String userName, UserLookup userLookup) {
        Symbol pattern = arguments.get(1);
        if (pattern instanceof Input) {
            Object value = ((Input<?>) pattern).value();
            if (value == null) {
                return this;
            }
            return new CompiledLike(signature, boundSignature, (String) value, caseSensitivity);
        }
        return super.compile(arguments, userName, userLookup);
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>... args) {
        assert args != null : "args must not be null";
        assert args.length == 2 : "number of args must be 2";

        String expression = args[0].value();
        String pattern = args[1].value();
        if (expression == null || pattern == null) {
            return null;
        }
        return matcher.test(expression, pattern, caseSensitivity);
    }

    @Override
    public Query toQuery(Reference ref, Literal<?> literal) {
        Object value = literal.value();
        assert value instanceof String
            : "LikeOperator is registered for string types. Value must be a string";
        return caseSensitivity.likeQuery(ref.storageIdent(), (String) value, ref.indexType() != IndexType.NONE);
    }

    private static class CompiledLike extends Scalar<Boolean, String> {
        private final Pattern pattern;

        CompiledLike(Signature signature, BoundSignature boundSignature, String pattern, CaseSensitivity caseSensitivity) {
            super(signature, boundSignature);
            this.pattern = LikeOperators.makePattern(pattern, caseSensitivity);
        }

        @SafeVarargs
        @Override
        public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>... args) {
            String value = args[0].value();
            if (value == null) {
                return null;
            }
            return pattern.matcher(value).matches();
        }
    }
}
