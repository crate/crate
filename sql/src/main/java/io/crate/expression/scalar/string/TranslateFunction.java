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
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.scalar.TripleScalar;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.types.DataTypes;

import java.util.HashMap;
import java.util.Locale;
import java.util.function.Consumer;

public final class TranslateFunction {
    public static void register(ScalarFunctionModule module) {
        var functionInfo = new FunctionInfo(new FunctionIdent("translate", ImmutableList.of(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING)), DataTypes.STRING);
        module.register(new TripleScalar<>(functionInfo, TranslateFunction::translate));
    }

    private static String translate(String text, String from, String to) {
        if (from.isEmpty() || text.isEmpty()) {
            return text;
        } else {
            return applyTranslationsToText(text, createTranslationMap(from, to));
        }
    }

    private static HashMap<Character, Consumer<StringBuilder>> createTranslationMap(String from, String to) {
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
                translationMap.put(fromChar, (sb) -> {});
            }
        }
        return translationMap;
    }

    private static String applyTranslationsToText(String text, HashMap<Character, Consumer<StringBuilder>> translationMap) {
        var outputSb = new StringBuilder();

        for (var ch: text.toCharArray()) {
            if (translationMap.containsKey(ch)) {
                translationMap.get(ch).accept(outputSb);
            } else {
                outputSb.append(ch);
            }
        }

        return outputSb.toString();
    }

    /**
     * Repeated occurrence of any char in the 'from' argument creates an ambiguity of resolving a correct translation.
     * Therefore, such arguments treated as invalid.
     */
    private static void checkDuplicatesInFromArg(Character ch, HashMap<Character, Consumer<StringBuilder>> translationMap) {
        if (translationMap.containsKey(ch)) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Argument 'from' contains duplicate characters '%s'", ch)
            );
        }
    }
}
