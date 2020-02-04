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

/**
 * A simple implementation of Translation map that
 * - resembles PostgreSQL implementation
 * - does not use compile()
 */

public class TranslateFunction {

    public static void register(ScalarFunctionModule module) {
        var functionInfo = new FunctionInfo(new FunctionIdent("translate", ImmutableList.of(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING)), DataTypes.STRING);
        module.register(new TripleScalar<>(functionInfo, TranslateFunction::translate));
    }

    private static String translate(String text, String from, String to) {
        var resultSb = new StringBuilder();
        var fromLength = from.length();
        var toLength = to.length();

        for (int textIdx = 0; textIdx < text.length(); textIdx++) {
            var textChar = text.charAt(textIdx);

            int lookupIdx = 0;

            for (;lookupIdx < fromLength && textChar != from.charAt(lookupIdx); lookupIdx++);

            if (lookupIdx < fromLength) {
                // translation found, the char from "to" is appended, or skipped if there is not match in "to".
                if (lookupIdx < toLength) {
                    resultSb.append(to.charAt(lookupIdx));
                }
            } else {
                // translation for the char not found, therefore the current char is appended
                resultSb.append(textChar);
            }
        }
        return resultSb.toString();
    }
}
