/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.scalar;

import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;

class ArrayUpperFunction extends Scalar<Integer, Object[]> {

    public static final String NAME = "array_upper";
    private FunctionInfo functionInfo;

    public static FunctionInfo createInfo(List<DataType> types) {
        return new FunctionInfo(new FunctionIdent(NAME, types), DataTypes.INTEGER);
    }

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new ArrayBoundFunctionResolver(NAME, ArrayUpperFunction::new));
    }

    private ArrayUpperFunction(FunctionInfo functionInfo) {
        this.functionInfo = functionInfo;
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    @Override
    public Integer evaluate(Input[] args) {
        Object[] array = (Object[]) args[0].value();
        Object dimension1Indexed = args[1].value();
        if (array == null || array.length == 0 || dimension1Indexed == null) {
            return null;
        }

        // sql dimensions are 1 indexed
        int dimension = (int) dimension1Indexed - 1;

        try {
            Object dimensionValue = array[dimension];
            if (dimensionValue.getClass().isArray()) {
                Object[] dimensionArray = (Object[]) dimensionValue;
                return dimensionArray.length;
            }
            // it's a one dimension array so return the argument length
            return array.length;
        } catch (ArrayIndexOutOfBoundsException e) {
            return null;
        }
    }
}
