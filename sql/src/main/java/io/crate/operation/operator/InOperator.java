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
package io.crate.operation.operator;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.Input;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;

import java.util.Set;

public class InOperator extends Operator<Object> {

    public static final String NAME = "op_in";

    private final FunctionInfo info;

    public InOperator(FunctionInfo info) {
        this.info = info;
    }

    public static void register(OperatorModule module) {
        for (DataType type : DataTypes.PRIMITIVE_TYPES) {
            FunctionInfo functionInfo = new FunctionInfo(
                    new FunctionIdent(NAME, ImmutableList.<DataType>of(type, new SetType(type))), DataTypes.BOOLEAN);
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
        if (!left.symbolType().isValueSymbol()) {
            return function;
        }
        Object inValue = ((Literal) left).value();
        Literal inList = (Literal) function.arguments().get(1);
        assert inList.valueType().id() == SetType.ID;
        Set values = (Set)inList.value();

        if (!values.contains(inValue)) {
            return Literal.newLiteral(false);
        }

        return Literal.newLiteral(true);
    }

    @Override
    public Boolean evaluate(Input<Object>... args) {
        assert (args != null);
        assert (args.length == 2);
        assert (args[0] != null && args[1] != null);

        Object inValue = args[0].value();
        Set<?> inList = (Set<?>)args[1].value();

        if (inValue == null || inList == null || inList.contains(null)) {
            return null;
        }

        return inList.contains(inValue);
    }

}
