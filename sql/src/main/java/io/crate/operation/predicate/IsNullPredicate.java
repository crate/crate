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

package io.crate.operation.predicate;

import com.google.common.base.Preconditions;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;

import static io.crate.planner.symbol.Field.unwrap;

public class IsNullPredicate<T> extends Scalar<Boolean, T> {

    public static final String NAME = "op_isnull";
    private final FunctionInfo info;

    public static void register(PredicateModule module) {
        module.register(NAME, new Resolver());
    }

    private static FunctionInfo generateInfo(List<DataType> types) {
        return new FunctionInfo(new FunctionIdent(NAME, types), DataTypes.BOOLEAN, FunctionInfo.Type.PREDICATE);
    }

    IsNullPredicate(FunctionInfo info) {
        this.info = info;
    }


    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        assert (symbol != null);
        assert (symbol.arguments().size() == 1);

        Symbol arg = unwrap(symbol.arguments().get(0));
        if (arg.equals(Literal.NULL) || arg.symbolType() == SymbolType.DYNAMIC_REFERENCE) {
            return Literal.newLiteral(true);
        } else if (arg.symbolType().isValueSymbol()) {
            return Literal.newLiteral(((Input) arg).value() == null);
        }
        return symbol;
    }

    @Override
    public Boolean evaluate(Input[] args) {
        assert args.length == 1;
        return args[0] == null || args[0].value() == null;
    }

    private static class Resolver implements DynamicFunctionResolver {

        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            Preconditions.checkArgument(
                dataTypes.size() == 1, "the is null predicate takes only 1 argument");

            return new IsNullPredicate<>(generateInfo(dataTypes));
        }
    }
}
