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

package io.crate.operation.operator;

import com.google.common.base.Preconditions;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;


public class EqOperator extends CmpOperator implements Scalar<Boolean, Object> {

    public static final String NAME = "op_=";

    private static final EqOperatorResolver dynamicResolver = new EqOperatorResolver();

    public static void register(OperatorModule module) {
        module.registerDynamicOperatorFunction(NAME, dynamicResolver);
    }

    @Override
    protected boolean compare(int comparisonResult) {
        return comparisonResult == 0;
    }

    protected EqOperator(FunctionInfo info) {
        super(info);
    }

    @Override
    public Boolean evaluate(Input<Object>... args) {
        assert args.length == 2;
        Object left = args[0].value();
        if (left == null){
            return null;
        }
        Object right = args[1].value();
        if (right == null){
            return null;
        }
        return left.equals(right);
    }

    static class EqOperatorResolver implements DynamicFunctionResolver {

        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            Preconditions.checkArgument(dataTypes.size() == 2);
            return new EqOperator(new FunctionInfo(new FunctionIdent(NAME, dataTypes), DataTypes.BOOLEAN));
        }
    }
}
