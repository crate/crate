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
import java.util.Optional;

/**
 * An option of translate() that
 * - is based on a map for a lookup
 * - does not use compile optimisation
 */

public final class TranslateFunctionUnoptimisedMap {

    public static void register(ScalarFunctionModule module) {
        var functionInfo = new FunctionInfo(new FunctionIdent("translate", ImmutableList.of(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING)), DataTypes.STRING);
        module.register(new TripleScalar<>(functionInfo, TranslateFunctionUnoptimisedMap::translate));
    }

    private static String translate(String text, String from, String to) {
        if (from.isEmpty() || text.isEmpty()) {
            return text;
        } else {
            return applyTranslationsToText(text, createTranslationMap(from, to));
        }
    }

    private static HashMap<Character, Optional<Character>> createTranslationMap(String from, String to) {
        var translationMap = new HashMap<Character, Optional<Character>>();
        var fromLength = from.length();
        var toLength = to.length();

        for (int i = 0; i < fromLength; i++) {
            Character fromChar = from.charAt(i);
            /*
             * we handle duplicate 'from' chars by using the first occurrence and dropping the consequent ones,
             * so that we could preserve the compatibility of behaviour with PostgreSQL implementation
             */
            if (i < toLength) {
                final var toChar = to.charAt(i);
                translationMap.putIfAbsent(fromChar, Optional.of(toChar));
            } else {
                translationMap.putIfAbsent(fromChar, Optional.empty());
            }
        }
        return translationMap;
    }

    private static String applyTranslationsToText(String text, HashMap<Character, Optional<Character>> translationMap) {
        var outputSb = new StringBuilder();

        for (var ch: text.toCharArray()) {
            var optToChar = translationMap.get(ch);
            if (optToChar != null) {
                optToChar.ifPresent(outputSb::append);
            } else {
                outputSb.append(ch);
            }
        }

        return outputSb.toString();
    }
}
