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

package io.crate.expression.operator;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.core.collections.MapComparator;
import io.crate.data.Input;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.crate.expression.operator.CmpOperator.CmpResolver.createInfo;

public class EqOperator extends CmpOperator {

    public static final String NAME = "op_=";

    private static final EqOperatorResolver dynamicResolver = new EqOperatorResolver(NAME);

    public static void register(OperatorModule module) {
        module.registerDynamicOperatorFunction(NAME, dynamicResolver);
    }

    public static Function createFunction(Symbol left, Symbol right) {
        return new Function(createInfo(NAME, Arrays.asList(left.valueType(), right.valueType())),
            Arrays.asList(left, right));
    }

    @Override
    protected boolean compare(int comparisonResult) {
        return comparisonResult == 0;
    }

    protected EqOperator(FunctionInfo info) {
        super(info);
    }

    @Override
    public Boolean evaluate(Input[] args) {
        assert args.length == 2 : "number of args must be 2";
        Object left = args[0].value();
        if (left == null) {
            return null;
        }
        Object right = args[1].value();
        if (right == null) {
            return null;
        }
        return left.equals(right);
    }

    private static class ArrayEqOperator extends CmpOperator {

        ArrayEqOperator(FunctionInfo info) {
            super(info);
        }

        @Override
        protected boolean compare(int comparisonResult) {
            return comparisonResult == 0;
        }

        @Override
        public Boolean evaluate(Input[] args) {
            Object[] left = (Object[]) args[0].value();
            if (left == null) {
                return null;
            }
            Object[] right = (Object[]) args[1].value();
            if (right == null) {
                return null;
            }
            return Arrays.deepEquals(left, right);
        }
    }

    private static class ObjectEqOperator extends Operator<Object> {

        private final FunctionInfo info;

        ObjectEqOperator(FunctionInfo info) {
            this.info = info;
        }

        @Override
        @SafeVarargs
        public final Boolean evaluate(Input<Object>... args) {
            Object left = args[0].value();
            Object right = args[1].value();
            if (left == null || right == null) {
                return null;
            }
            return MapComparator.compareMaps(((Map) left), ((Map) right)) == 0;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }
    }

    private static class EqOperatorResolver extends CmpResolver {

        EqOperatorResolver(String name) {
            super(name,
                FuncParams.builder(Param.ANY, Param.ANY).build(),
                info -> {
                    List<DataType> dataTypes = info.ident().argumentTypes();

                    DataType leftType = dataTypes.get(0);
                    DataType rightType = dataTypes.get(1);

                    if (DataTypes.isCollectionType(leftType) && DataTypes.isCollectionType(rightType)) {
                        return new ArrayEqOperator(info);
                    }
                    if (leftType.equals(DataTypes.OBJECT) && rightType.equals(DataTypes.OBJECT)) {
                        return new ObjectEqOperator(info);
                    }
                    return new EqOperator(info);
                });
        }

    }
}
