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

package io.crate.operation.operator;

import io.crate.metadata.FunctionInfo;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;

import java.util.regex.Pattern;


public class RegexpMatchCaseInsensitiveOperator extends Operator<BytesRef> {

    public static final String NAME = "op_~*";
    public static final FunctionInfo INFO = generateInfo(NAME, DataTypes.STRING);

    public static void register(OperatorModule module) {
        module.registerOperatorFunction(new RegexpMatchCaseInsensitiveOperator());
    }


    @Override
    public Boolean evaluate(Input<BytesRef>... args) {
        assert args.length == 2 : "invalid number of arguments";
        BytesRef source = args[0].value();
        if (source == null) {
            return null;
        }
        BytesRef pattern = args[1].value();
        if (pattern == null) {
            return null;
        }

        Pattern p = Pattern.compile(pattern.utf8ToString(), Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
        return p.matcher(source.utf8ToString()).matches();
    }

    @Override
    public FunctionInfo info() {
        return INFO;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        assert (symbol != null);
        assert symbol.arguments().size() == 2;

        if (anyNonLiterals(symbol.arguments())) {
            return symbol;
        }

        if (containsNullLiteral(symbol.arguments())) {
            return Literal.NULL;
        }

        return Literal.newLiteral(
                evaluate(
                        (Literal) symbol.arguments().get(0),
                        (Literal) symbol.arguments().get(1)
                )
        );
    }
}
