/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.scalar.string;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Chars;
import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.sql.tree.TrimMode;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.HashSet;
import java.util.List;
import java.util.function.BiFunction;


public final class TrimFunctions {

    private static final String TRIM_NAME = "trim";
    private static final String LTRIM_NAME = "ltrim";
    private static final String RTRIM_NAME = "rtrim";

    public static void register(ScalarFunctionModule module) {
        module.register(TRIM_NAME, new BaseFunctionResolver(FuncParams
            .builder(Param.STRING)
            .withVarArgs(Param.STRING).limitVarArgOccurrences(2)
            .build()) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> datatypes) {
                if (datatypes.size() == 1) {
                    return new OneCharTrimFunction(
                        new FunctionInfo(
                            new FunctionIdent(TRIM_NAME, ImmutableList.of(datatypes.get(0))),
                            datatypes.get(0)),
                        ' ');
                } else {
                    return new TrimFunction(
                        new FunctionInfo(new FunctionIdent(TRIM_NAME, datatypes), datatypes.get(0))
                    );
                }
            }
        });

        module.register(LTRIM_NAME, new SingleSideTrimFunctionResolver((i, c) -> trimChars(i, c, TrimMode.LEADING)));
        module.register(RTRIM_NAME, new SingleSideTrimFunctionResolver((i, c) -> trimChars(i, c, TrimMode.TRAILING)));
    }

    private static class TrimFunction extends Scalar<String, String> {

        private final FunctionInfo info;

        TrimFunction(FunctionInfo info) {
            this.info = info;
        }

        @Override
        public FunctionInfo info() {
            return info;
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
                return new OneCharTrimFunction(info(), charsToTrim.charAt(0));
            }
            return this;
        }

        @Override
        public String evaluate(TransactionContext txnCtx, Input<String>[] args) {
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

        private final FunctionInfo info;
        private final char charToTrim;

        OneCharTrimFunction(FunctionInfo info, char charToTrim) {
            this.info = info;
            this.charToTrim = charToTrim;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @Override
        public String evaluate(TransactionContext txnCtx, Input<String>[] args) {
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

        private final FunctionInfo info;
        private final BiFunction<String, String, String> trimFunction;

        SingleSideTrimFunction(FunctionInfo info,
                               BiFunction<String, String, String> function) {
            this.info = info;
            this.trimFunction = function;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @Override
        public String evaluate(TransactionContext txnCtx, Input<String>[] args) {
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

    private static class SingleSideTrimFunctionResolver extends BaseFunctionResolver {
        private final BiFunction<String, String, String> trimFunction;

        SingleSideTrimFunctionResolver(BiFunction<String, String, String> trimFunction) {
            super(
                FuncParams
                    .builder(Param.STRING)
                    .withVarArgs(Param.STRING)
                    .limitVarArgOccurrences(1)
                    .build()
            );
            this.trimFunction = trimFunction;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return new SingleSideTrimFunction(
                new FunctionInfo(
                    new FunctionIdent(LTRIM_NAME, dataTypes), DataTypes.STRING
                ),
                trimFunction
            );
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
