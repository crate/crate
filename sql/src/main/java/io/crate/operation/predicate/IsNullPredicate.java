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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.crate.DataType;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.planner.symbol.BooleanLiteral;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;

import javax.annotation.Nullable;
import java.util.Arrays;

public class IsNullPredicate<T> implements Scalar<Boolean, T> {

    private static final Predicate<DataType> IS_OBJECT_TYPE = new Predicate<DataType>() {
        @Override
        public boolean apply(@Nullable DataType input) {
            return input != null && input.elementType() != DataType.OBJECT;
        }
    };

    public static final String NAME = "op_isnull";
    private final FunctionInfo info;

    public static void register(PredicateModule module) {
        for (DataType type : Iterables.filter(
                Iterables.concat(DataType.ARRAY_TYPES, DataType.SET_TYPES, DataType.PRIMITIVE_TYPES),
                IS_OBJECT_TYPE)
        ) {
            module.registerPredicateFunction(new IsNullPredicate(generateInfo(type)));
        }
        module.registerPredicateFunction(new IsNullPredicate(generateInfo(DataType.NULL)));

    }

    private static FunctionInfo generateInfo(DataType type) {
        return new FunctionInfo(new FunctionIdent(NAME, Arrays.asList(type)), DataType.BOOLEAN);
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

        Symbol arg = symbol.arguments().get(0);
        if (arg.symbolType() == SymbolType.NULL_LITERAL) {
            return BooleanLiteral.TRUE;
        } else if (arg.symbolType().isLiteral()) {
            return BooleanLiteral.FALSE;
        }

        return symbol;
    }

    @Override
    public Boolean evaluate(Input<T>... args) {
        assert args.length == 1;
        return args[0] == null;
    }
}
