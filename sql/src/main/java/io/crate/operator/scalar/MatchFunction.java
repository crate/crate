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

package io.crate.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operator.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.Inject;

public class MatchFunction implements Scalar<Boolean> {

    public static final String NAME = "match";

    public static void register(ScalarFunctionModule module) {
        ImmutableList<FunctionIdent> functionIdents = ImmutableList.of(
                new FunctionIdent(MatchFunction.NAME, ImmutableList.of(DataType.STRING, DataType.STRING)),
                new FunctionIdent(MatchFunction.NAME, ImmutableList.of(DataType.NULL, DataType.STRING))
        );

        for (FunctionIdent functionIdent : functionIdents) {
            module.registerScalarFunction(new MatchFunction(new FunctionInfo(functionIdent, DataType.BOOLEAN)));
        }
    }

    private final FunctionInfo info;

    @Inject
    public MatchFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public Boolean evaluate(Input<?>... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function function) {
        assert (function != null);
        assert (function.arguments().size() == 2);

        Symbol left = function.arguments().get(0);
        Symbol right = function.arguments().get(1);

        if (left.symbolType() != SymbolType.STRING_LITERAL && right.symbolType() == SymbolType.STRING_LITERAL) {
            return function;
        } else {
            throw new UnsupportedOperationException(
                    "Query does not fulfil the MATCH('index name', 'query term') specification."
            );
        }
    }
}
