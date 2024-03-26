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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.expression.operator.LikeOperators.CaseSensitivity;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.role.Roles;

public class LikeOperator extends Operator<String> {

    private final FourPredicate<String, String, Character, CaseSensitivity> matcher;
    private final CaseSensitivity caseSensitivity;

    private final java.util.function.Function<Input<String>[], Character> escapeFromInputs;
    private final java.util.function.Function<List<Symbol>, Character> escapeFromSymbols;

    private final int evaluateArgsCount;

    public LikeOperator(Signature signature,
                        BoundSignature boundSignature,
                        FourPredicate<String, String, Character, CaseSensitivity> matcher,
                        CaseSensitivity caseSensitivity) {
        super(signature, boundSignature);
        this.matcher = matcher;
        this.caseSensitivity = caseSensitivity;
        this.evaluateArgsCount = signature.getArgumentDataTypes().size();
        if (signature.getArgumentDataTypes().size() == 3) {
            escapeFromInputs = inputs -> validateAndGetEscape(inputs[2]);
            escapeFromSymbols = symbols -> {
                Symbol escape = symbols.get(2);
                assert escape instanceof Literal<?> : "Escape character must be a literal";
                return validateAndGetEscape((Literal<?>) escape);
            };
        } else {
            // BWC branch for pre-5.6 signature without ESCAPE.
            escapeFromInputs = (ignored) -> LikeOperators.DEFAULT_ESCAPE;
            escapeFromSymbols = (ignored) -> LikeOperators.DEFAULT_ESCAPE;
        }
    }


    /**
     * @return escape character.
     * NULL indicates disabled escaping.
     */
    @Nullable
    private static Character validateAndGetEscape(Input<?> escapeInput) {
        String value = (String) escapeInput.value();
        if (value.length() > 1) {
            throw new IllegalArgumentException("ESCAPE must be a single character");
        }
        if (value.isEmpty()) {
            return null;
        }
        return value.charAt(0);
    }

    @Override
    public Scalar<Boolean, String> compile(List<Symbol> arguments, String userName, Roles roles) {
        Symbol pattern = arguments.get(1);
        if (pattern instanceof Input) {
            Object value = ((Input<?>) pattern).value();
            if (value == null) {
                return this;
            }
            Character escapeChar = escapeFromSymbols.apply(arguments);
            return new CompiledLike(signature, boundSignature, (String) value, escapeChar, caseSensitivity);
        }
        return super.compile(arguments, userName, roles);
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>... args) {
        assert args != null : "args must not be null";
        assert args.length == evaluateArgsCount : "number of args must be " + evaluateArgsCount;

        String expression = args[0].value();
        String pattern = args[1].value();
        if (expression == null || pattern == null) {
            return null;
        }
        Character escapeChar = escapeFromInputs.apply(args);
        return matcher.test(expression, pattern, escapeChar, caseSensitivity);
    }

    @Override
    @Nullable
    public Query toQuery(Function function, LuceneQueryBuilder.Context context) {
        List<Symbol> args = function.arguments();
        if (args.get(0) instanceof Reference ref
            && args.get(1) instanceof Literal<?> patternLiteral
        ) {
            Object value = patternLiteral.value();
            assert value instanceof String
                : "LikeOperator is registered for string types. Value must be a string";
            if (((String) value).isEmpty()) {
                return new TermQuery(new Term(ref.storageIdent(), ""));
            }
            Character escapeChar = escapeFromSymbols.apply(args);
            return caseSensitivity.likeQuery(ref.storageIdent(),
                (String) value,
                escapeChar,
                ref.indexType() != IndexType.NONE
            );
        }
        return null;
    }

    private static class CompiledLike extends Scalar<Boolean, String> {
        private final Pattern pattern;

        CompiledLike(Signature signature,
                     BoundSignature boundSignature,
                     String pattern,
                     Character escapeChar,
                     CaseSensitivity caseSensitivity) {
            super(signature, boundSignature);
            this.pattern = LikeOperators.makePattern(pattern, caseSensitivity, escapeChar);
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
