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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

public class ArraySortFunction extends Scalar<Object[], Object[]> {

    public static final String NAME = "array_sort";
    private static final Set<DataType> ALLOWED_TYPES = ImmutableSet.<DataType>builder()
        .addAll(DataTypes.PRIMITIVE_TYPES)
        .add(DataTypes.UNDEFINED)
        .build();
    private FunctionInfo functionInfo;

    public static void register(ScalarFunctionModule module) {
        for (DataType dataType : ALLOWED_TYPES) {
            DataType arrayDataType = new ArrayType(dataType);
            FunctionIdent ident = new FunctionIdent(NAME, ImmutableList.of(arrayDataType, DataTypes.STRING));
            FunctionIdent identAsc = new FunctionIdent(NAME, ImmutableList.of(arrayDataType));
            module.register(new ArraySortFunction(new FunctionInfo(ident, arrayDataType)));
            module.register(new ArraySortFunction(new FunctionInfo(identAsc, arrayDataType)));
        }
    }

    protected ArraySortFunction(FunctionInfo functionInfo) {
        this.functionInfo = functionInfo;
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    @Override
    public Object[] evaluate(Input[] args) {
        assert args.length >= 1;
        Object arrayValue = args[0].value();
        if (arrayValue == null) {
            return null;
        }

        Object[] array = (Object[]) arrayValue;
        if (args.length == 2) {
            String direction = BytesRefs.toString(args[1].value());
            if (reverseDirection(direction)) {
                Arrays.sort(array, Collections.reverseOrder());
            }
        }
        Arrays.sort(array);
        return array;
    }

    private boolean reverseDirection(String direction) {
        return "desc".equalsIgnoreCase(direction);
    }
}
