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
import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataTypes;

import java.util.Arrays;
import java.util.List;

/**
 * An option of translate() that
 * - is based on an ordered char array for a map lookup
 * - use compile optimisation
 */

public class TranslateFunctionOptimisedArray extends Scalar<String, String> {

    private final FunctionInfo functionInfo = new FunctionInfo(
        new FunctionIdent("translate", ImmutableList.of(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING)),
        DataTypes.STRING);

    public TranslateFunctionOptimisedArray() {
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    public static void register(ScalarFunctionModule module) {
        module.register(new TranslateFunctionOptimisedArray());
    }

    @Override
    public Scalar<String, String> compile(List<Symbol> arguments) {
        assert arguments.size() == 3 : "function's number of arguments must be 3";

        Symbol fromSymbol = arguments.get(1);
        if (!Literal.isLiteral(fromSymbol, DataTypes.STRING)) {
            return this;
        }

        Symbol toSymbol = arguments.get(2);
        if (!Literal.isLiteral(toSymbol, DataTypes.STRING)) {
            return this;
        }

        var fromStr = (String) ((Input) fromSymbol).value();
        var toStr = (String) ((Input) toSymbol).value();

        var sortedFromArray = getSortedFromArray(fromStr);
        var lookupIndexes = getLookupIndexes(sortedFromArray, fromStr, toStr);

        return new WithPrecomputedTranslate(sortedFromArray, lookupIndexes);
    }

    @Override
    public String evaluate(TransactionContext txnCtx, Input<String>... args) {
        var text = args[0].value();
        if (text == null) {
            return null;
        }

        var from = args[1].value();
        if (from == null) {
            return null;
        }

        var to = args[2].value();
        if (to == null) {
            return null;
        }

        if (text.isEmpty() || from.isEmpty()) {
            return text;
        } else {
            var sortedFromArray = getSortedFromArray(from);
            var lookupIndexes = getLookupIndexes(sortedFromArray, from, to);

            return translate(text, to, sortedFromArray, lookupIndexes);
        }
    }

    private class WithPrecomputedTranslate extends TranslateFunctionOptimisedArray {
        private final char[] sortedFromArray;
        private final int[] lookupIndexes;

        private WithPrecomputedTranslate(char[] sortedFromArray, int[] lookupIndexes) {
            this.sortedFromArray = sortedFromArray;
            this.lookupIndexes = lookupIndexes;
        }

        @SafeVarargs
        @Override
        public final String evaluate(TransactionContext txnCtx, Input<String>... args) {
            var text = args[0].value();
            var to = args[2].value();

            if (text == null) {
                return null;
            }

            if (text.isEmpty()) {
                return text;
            }

            return translate(text, to, sortedFromArray, lookupIndexes);
        }
    }

    private char[] getSortedFromArray(String from) {
        var charArray = from.toCharArray();
        Arrays.sort(charArray);
        return charArray;
    }

    private int[] getLookupIndexes(char[] sortedFromArray, String from, String to) {
        var toLength = to.length();
        var lookupIndexes = new int [sortedFromArray.length];

        for (int i = 0; i < sortedFromArray.length; i++) {
            var ch = from.charAt(i);
            var lookupIdx = Arrays.binarySearch(sortedFromArray, ch);
            if (i < toLength) {
                lookupIndexes[lookupIdx] = i;
            } else {
                lookupIndexes[lookupIdx] = -1;
            }
        }

        return lookupIndexes;
    }

    private String translate(String text, String to, char[] sortedFromArray, int[] lookupIndexes) {
        var sb = new StringBuilder();
        for (int i = 0; i < text.length(); i++) {
            var ch = text.charAt(i);
            var lookupIdx = Arrays.binarySearch(sortedFromArray, ch);
            if (lookupIdx >= 0) {
                var valueIdx = lookupIndexes[lookupIdx];
                if (valueIdx >= 0) {
                    sb.append(to.charAt(valueIdx));
                }
            } else {
                sb.append(ch);
            }
        }
        return sb.toString();
    }
}
