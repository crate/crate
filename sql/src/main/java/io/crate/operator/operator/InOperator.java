/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operator.Input;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;

import java.util.Set;

public class InOperator extends Operator<Object> {

    public static final String NAME = "op_in";

    private final FunctionInfo info;

    public InOperator(FunctionInfo info) {
        this.info = info;
    }

    public static void register(OperatorModule module) {
        for (DataType type : DataType.ALL_TYPES) {
            DataType setType = DataType.SET_TYPES.get(type.ordinal());
            FunctionInfo functionInfo = new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(type, setType)), DataType.BOOLEAN);
            module.registerOperatorFunction(new InOperator(functionInfo));
        }
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function function) {
        // ... where id in (1,2,3,4,...)
        // arguments.get(0) ... id
        // arguments.get(1) ... SetLiteral<Literal> (1,2,3,4,...)
        Symbol left = function.arguments().get(0);
        if (!left.symbolType().isLiteral()) {
            return function;
        }
        Literal inValue = (Literal) left;
        SetLiteral inList = (SetLiteral) function.arguments().get(1);

        // not in list if data types do not match.
        if (!inList.contains(inValue)) {
            return new BooleanLiteral(false);
        }

        return new BooleanLiteral(true);
    }

    @Override
    public Boolean evaluate(Input<Object>... args) {
        assert (args != null);
        assert (args.length == 2);

        if (args[0] == null) {
            throw new IllegalArgumentException("<expression> IN <pattern>: arg[0] must not be null");
        }
        if (args[1] == null || args[1].value() == null) {
            throw new IllegalArgumentException("<expression> IN <pattern>: pattern must not be null");
        }
        if (!(args[1].value() instanceof Set)) {
            throw new IllegalArgumentException("<expression> IN <pattern>: pattern must be of type Set");
        }

        Object inValue = args[0].value();
        Set<?> inList = (Set<?>)args[1].value();

        return inList.contains(inValue);
    }

}
