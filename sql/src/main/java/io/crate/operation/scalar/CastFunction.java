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

package io.crate.operation.scalar;

import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;

import java.util.List;

public class CastFunction implements Scalar<Object, Object> {

    public static final String NAME = "cast";

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    protected final FunctionInfo info;

    public CastFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Object evaluate(Input[] args) {
        assert args.length >= 1;
        return info.returnType().value(args[0].value());
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        assert symbol.arguments().size() >= 1;

        Symbol left = symbol.arguments().get(0);
        if (left.symbolType().isValueSymbol()) {
            return Literal.toLiteral(left, info.returnType());
        }

        return symbol;
    }

    private static class Resolver implements DynamicFunctionResolver {

        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            assert dataTypes.size() == 2;
            return new CastFunction(new FunctionInfo(new FunctionIdent(NAME, dataTypes), dataTypes.get(1)));
        }
    }
}
