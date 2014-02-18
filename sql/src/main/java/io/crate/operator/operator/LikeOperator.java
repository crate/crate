/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operator.operator;

import io.crate.metadata.FunctionInfo;
import io.crate.planner.symbol.BooleanLiteral;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.StringLiteral;
import io.crate.planner.symbol.Symbol;
import org.cratedb.DataType;

import java.util.regex.Pattern;

public class LikeOperator extends Operator {

    public static final String NAME = "op_like";

    private FunctionInfo info;

    private static final String ZERO_OR_MORE = "%";
    private static final String EXACTLY_ONE = "_";

    public static void register(OperatorModule module) {
        module.registerOperatorFunction(new LikeOperator(generateInfo(NAME, DataType.STRING)));
    }

    public LikeOperator(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        assert (symbol != null);
        assert (symbol.arguments().size() == 2);

        if (!symbol.arguments().get(0).symbolType().isLiteral()) {
            return symbol;
        }

        StringLiteral expression = (StringLiteral) symbol.arguments().get(0);
        StringLiteral pattern = (StringLiteral) symbol.arguments().get(1);

        return new BooleanLiteral(matches(expression, pattern));
    }

    private boolean matches(StringLiteral expression, StringLiteral pattern) {
        return Pattern.matches(replaceWildcards(expression), pattern.value().utf8ToString());
    }

    private String replaceWildcards(StringLiteral literal) {
        String s = literal.value().utf8ToString();
        return s.replaceAll(ZERO_OR_MORE, ".*").replaceAll(EXACTLY_ONE, ".");
    }

}
