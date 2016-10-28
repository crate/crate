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

package io.crate.operation.scalar.arithmetic;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Collections;
import java.util.List;
import java.util.Set;

abstract class ArithmeticFunction extends Scalar<Number, Number> {

    private final static Set<DataType> NUMERIC_WITH_DECIMAL =
        Sets.newHashSet(DataTypes.FLOAT, DataTypes.DOUBLE);
    private final static Set<DataType> ALLOWED_TYPES = Sets.newHashSet(
        Iterables.concat(DataTypes.NUMERIC_PRIMITIVE_TYPES, Collections.singletonList(DataTypes.TIMESTAMP))
    );

    protected final FunctionInfo info;

    ArithmeticFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    static FunctionInfo genDoubleInfo(String functionName, List<DataType> dataTypes) {
        return genDoubleInfo(functionName, dataTypes, FunctionInfo.DETERMINISTIC_ONLY);
    }

    static FunctionInfo genDoubleInfo(String functionName, List<DataType> dataTypes, Set<FunctionInfo.Feature> features) {
        return new FunctionInfo(new FunctionIdent(functionName, dataTypes), DataTypes.DOUBLE, FunctionInfo.Type.SCALAR, features);
    }

    static FunctionInfo genLongInfo(String functionName, List<DataType> dataTypes) {
        return genLongInfo(functionName, dataTypes, FunctionInfo.DETERMINISTIC_ONLY);
    }

    static FunctionInfo genLongInfo(String functionName, List<DataType> dataTypes, Set<FunctionInfo.Feature> features) {
        return new FunctionInfo(new FunctionIdent(functionName, dataTypes), DataTypes.LONG, FunctionInfo.Type.SCALAR, features);
    }

    static void validateTypes(List<DataType> dataTypes) {
        Preconditions.checkArgument(dataTypes.size() == 2);
        DataType leftType = dataTypes.get(0);
        DataType rightType = dataTypes.get(1);
        Preconditions.checkArgument(ALLOWED_TYPES.contains(leftType),
            "invalid type %s of left argument", leftType.toString());
        Preconditions.checkArgument(ALLOWED_TYPES.contains(rightType),
            "invalid type %s of right argument", leftType.toString());
    }

    static boolean containsTypesWithDecimal(List<DataType> dataTypes) {
        for (DataType dataType : dataTypes) {
            if (NUMERIC_WITH_DECIMAL.contains(dataType)) {
                return true;
            }
        }
        return false;
    }

}
