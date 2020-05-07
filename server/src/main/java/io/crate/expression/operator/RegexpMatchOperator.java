/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.data.Input;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataTypes;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;

import java.nio.charset.StandardCharsets;

import static io.crate.expression.scalar.regex.RegexMatcher.isPcrePattern;


public class RegexpMatchOperator extends Operator<String> {

    public static final String NAME = "op_~";
    public static final FunctionInfo INFO = generateInfo(NAME, DataTypes.STRING);

    public static void register(OperatorModule module) {
        module.registerOperatorFunction(new RegexpMatchOperator());
    }


    @Override
    public Boolean evaluate(TransactionContext txnCtx, Input<String>... args) {
        assert args.length == 2 : "invalid number of arguments";
        String source = args[0].value();
        if (source == null) {
            return null;
        }
        String pattern = args[1].value();
        if (pattern == null) {
            return null;
        }
        if (isPcrePattern(pattern)) {
            return source.matches(pattern);
        } else {
            RegExp regexp = new RegExp(pattern);
            ByteRunAutomaton regexpRunAutomaton = new ByteRunAutomaton(regexp.toAutomaton());
            byte[] bytes = source.getBytes(StandardCharsets.UTF_8);
            return regexpRunAutomaton.run(bytes, 0, bytes.length);
        }
    }

    @Override
    public FunctionInfo info() {
        return INFO;
    }
}
