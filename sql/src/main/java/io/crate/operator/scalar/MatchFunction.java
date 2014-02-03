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

import com.google.common.base.Preconditions;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operator.Input;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.Inject;

import java.util.Arrays;
import java.util.Objects;

public class MatchFunction implements Scalar<Boolean> {

    public static final String NAME = "match";
    public static final FunctionInfo INFO = new FunctionInfo(
            new FunctionIdent(NAME, Arrays.asList(DataType.STRING, DataType.STRING)),
            DataType.BOOLEAN
    );

    @Inject
    public MatchFunction() {}

    @Override
    public Boolean evaluate(Input<?>... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionInfo info() {
        return INFO;
    }

    @Override
    public Symbol normalizeSymbol(Function function) {
        Preconditions.checkNotNull(function);
        Preconditions.checkArgument(function.arguments().size() == 2);

        Symbol left = function.arguments().get(0);
        Symbol right = function.arguments().get(1);

        if (left.symbolType() == SymbolType.STRING_LITERAL && right.symbolType() == SymbolType.STRING_LITERAL) {
            return new BooleanLiteral(Objects.equals(left, right));
        }

        return function;
    }
}
