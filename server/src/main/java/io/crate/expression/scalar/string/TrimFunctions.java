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

package io.crate.expression.scalar.string;

import com.google.common.primitives.Chars;
import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.TrimMode;
import io.crate.types.DataTypes;

import java.util.HashSet;
import java.util.List;
import java.util.function.BiFunction;


public final class TrimFunctions {

    private static final String TRIM_NAME = "trim";
    private static final String LTRIM_NAME = "ltrim";
    private static final String RTRIM_NAME = "rtrim";

    public static void register(ScalarFunctionModule module) {
        // trim(text)
        module.register(
            Signature.scalar(
                TRIM_NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, boundSignature) ->
                new OneCharTrimFunction(
                    signature,
                    boundSignature,
                    ' '
                )
        );
        // trim(MODE trimmingText from text)
        module.register(
            Signature.scalar(
                TRIM_NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            TrimFunction::new
        );

        // ltrim(text)
        module.register(
            Signature.scalar(
                LTRIM_NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, boundSignature) ->
                new SingleSideTrimFunction(
                    signature,
                    boundSignature,
                    (i, c) -> trimChars(i, c, TrimMode.LEADING)
                )
        );
        // ltrim(text, trimmingText)
        module.register(
            Signature.scalar(
                LTRIM_NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, boundSignature) ->
                new SingleSideTrimFunction(
                    signature,
                    boundSignature,
                    (i, c) -> trimChars(i, c, TrimMode.LEADING)
                )
        );

        // rtrim(text)
        module.register(
            Signature.scalar(
                RTRIM_NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, boundSignature) ->
                new SingleSideTrimFunction(
                    signature,
                    boundSignature,
                    (i, c) -> trimChars(i, c, TrimMode.TRAILING)
                )
        );
        // rtrim(text, trimmingText)
        module.register(
            Signature.scalar(
                RTRIM_NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, boundSignature) ->
                new SingleSideTrimFunction(
                    signature,
                    boundSignature,
                    (i, c) -> trimChars(i, c, TrimMode.TRAILING)
                )
        );
    }

    private static class TrimFunction extends Scalar<String, String> {

        private final Signature signature;
        private final Signature boundSignature;

        TrimFunction(Signature signature, Signature boundSignature) {
            this.signature = signature;
            this.boundSignature = boundSignature;
        }

        @Override
        public Signature signature() {
            return signature;
        }

        @Override
        public Signature boundSignature() {
            return boundSignature;
        }

        @Override
        public Scalar<String, String> compile(List<Symbol> arguments) {
            assert arguments.size() == 3 : "number of args must be 3";

            Symbol modeSymbol = arguments.get(2);
            if (!Literal.isLiteral(modeSymbol, DataTypes.STRING)) {
                return this;
            }
            TrimMode mode = TrimMode.of((String) ((Input) modeSymbol).value());
            if (mode != TrimMode.BOTH) {
                return this;
            }

            Symbol charsToTrimSymbol = arguments.get(1);
            if (!Literal.isLiteral(charsToTrimSymbol, DataTypes.STRING)) {
                return this;
            }

            String charsToTrim = (String) ((Input) charsToTrimSymbol).value();
            if (charsToTrim.length() == 1) {
                return new OneCharTrimFunction(signature, boundSignature, charsToTrim.charAt(0));
            }
            return this;
        }

        @Override
        public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>[] args) {
            assert args.length == 3 : "number of args must be 3";
            String target = args[0].value();
            if (target == null) {
                return null;
            }

            String charsToTrimArg = args[1].value();
            if (charsToTrimArg == null) {
                return target;
            }

            TrimMode mode = TrimMode.of(args[2].value());
            return trimChars(target, charsToTrimArg, mode);
        }
    }

    private static class OneCharTrimFunction extends Scalar<String, String> {

        private final Signature signature;
        private final Signature boundSignature;
        private final char charToTrim;

        OneCharTrimFunction(Signature signature, Signature boundSignature, char charToTrim) {
            this.signature = signature;
            this.boundSignature = boundSignature;
            this.charToTrim = charToTrim;
        }

        @Override
        public Signature signature() {
            return signature;
        }

        @Override
        public Signature boundSignature() {
            return boundSignature;
        }

        @Override
        public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>[] args) {
            String target = args[0].value();
            if (target == null) {
                return null;
            }

            int start = 0;
            int len = target.length();

            while (start < len && target.charAt(start) == charToTrim) {
                start++;
            }

            while (start < len && target.charAt(len - 1) == charToTrim) {
                len--;
            }

            return target.substring(start, len);
        }
    }

    private static class SingleSideTrimFunction extends Scalar<String, String> {

        private final Signature signature;
        private final Signature boundSignature;
        private final BiFunction<String, String, String> trimFunction;

        SingleSideTrimFunction(Signature signature,
                               Signature boundSignature,
                               BiFunction<String, String, String> function) {
            this.signature = signature;
            this.boundSignature = boundSignature;
            this.trimFunction = function;
        }

        @Override
        public Signature signature() {
            return signature;
        }

        @Override
        public Signature boundSignature() {
            return boundSignature;
        }

        @Override
        public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>[] args) {
            assert args.length == 1 || args.length == 2 : "number of args must be 1 or 2";

            String target = args[0].value();
            if (target == null) {
                return null;
            }

            if (args.length == 2) {
                String passedTrimmingText = args[1].value();
                if (passedTrimmingText != null) {
                    return trimFunction.apply(target, passedTrimmingText);
                }
            }

            return trimFunction.apply(target, " ");
        }
    }

    private static String trimChars(String target, String charsToTrimArg, TrimMode mode) {
        HashSet<Character> charsToTrim =
            new HashSet<>(Chars.asList(charsToTrimArg.toCharArray()));

        int start = mode.getStartIdx(target, charsToTrim);
        int len = mode.getTrimmedLength(target, charsToTrim);

        return start < len ? target.substring(start, len) : "";
    }
}
