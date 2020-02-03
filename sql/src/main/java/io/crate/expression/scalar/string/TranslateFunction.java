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

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;

public class TranslateFunction extends Scalar<String, String> {
    private final FunctionInfo functionInfo = new FunctionInfo(
        new FunctionIdent("translate", ImmutableList.of(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING)),
        DataTypes.STRING);

    public TranslateFunction() {
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    public static void register(ScalarFunctionModule module) {
        module.register(new TranslateFunction());
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
        return new WithPrecomputedTranslate(createTranslationMap(fromStr, toStr));
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
            return translate(text, from, to);
        }
    }

    private class WithPrecomputedTranslate extends TranslateFunction {
        private final HashMap<Character, Consumer<StringBuilder>> translationMap;

        private WithPrecomputedTranslate(HashMap<Character, Consumer<StringBuilder>> translationMap) {
            this.translationMap = translationMap;
        }

        @SafeVarargs
        @Override
        public final String evaluate(TransactionContext txnCtx, Input<String>... args) {
            var text = args[0].value();
            var from = args[1].value();
            var to = args[2].value();

            if (text == null) {
                return null;
            }

            if (text.isEmpty()) {
                return text;
            } else {
                return translate(text, from, to);
            }
        }
    }

    protected String translate(String text, String from, String to) {
        var resultSb = new StringBuilder();
        var fromChars = from.toCharArray();
        var fromLength = fromChars.length;
        var toChars = to.toCharArray();
        var toLength = toChars.length;

        for (int textIdx = 0; textIdx < text.length(); textIdx++) {
            var textChar = text.charAt(textIdx);

            int lookupIdx = 0;

            for (;lookupIdx < fromLength && textChar != fromChars[lookupIdx]; lookupIdx++);

            if (lookupIdx < fromLength) {
                // translation found, the char from "to" is appended, or skipped if there is not match in "to".
                if (lookupIdx < toLength) {
                    resultSb.append(toChars[lookupIdx]);
                }
            } else {
                // translation for the char not found, therefore the current char is appended
                resultSb.append(textChar);
            }
        }
        return resultSb.toString();
    }

    protected String translate(String text, HashMap<Character, Consumer<StringBuilder>> translationMap) {
        return applyTranslationsToText(text, translationMap);
    }

    private HashMap<Character, Consumer<StringBuilder>> createTranslationMap(String from, String to) {
        var translationMap = new HashMap<Character, Consumer<StringBuilder>>();
        var fromLength = from.length();
        var toLength = to.length();

        for (int i = 0; i < fromLength; i++) {
            Character fromChar = from.charAt(i);

            checkDuplicatesInFromArg(fromChar, translationMap);

            if (i < toLength) {
                final var toChar = to.charAt(i);
                translationMap.put(fromChar, (sb) -> sb.append(toChar));
            } else {
                translationMap.put(fromChar, (sb) -> {
                });
            }
        }
        return translationMap;
    }

    private String applyTranslationsToText(String text, HashMap<Character, Consumer<StringBuilder>> translationMap) {
        var outputSb = new StringBuilder();

        for (int i = 0; i < text.length(); i++) {
            var fromChar = text.charAt(i);
            var function = translationMap.get(fromChar);
            if (function != null) {
                function.accept(outputSb);
            } else {
                outputSb.append(fromChar);
            }
        }
        return outputSb.toString();
    }

    /**
     * Repeated occurrence of any char in the 'from' argument creates an ambiguity of resolving a correct translation.
     * Therefore, such arguments treated as invalid.
     */
    private void checkDuplicatesInFromArg(Character ch, HashMap<Character, Consumer<StringBuilder>> translationMap) {
        if (translationMap.containsKey(ch)) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Argument 'from' contains duplicate characters '%s'", ch)
            );
        }
    }
}
