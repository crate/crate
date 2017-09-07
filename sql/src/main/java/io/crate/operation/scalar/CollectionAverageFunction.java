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

package io.crate.operation.scalar;

import com.google.common.collect.ImmutableList;
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;

import java.util.Set;

public class CollectionAverageFunction extends Scalar<Double, Set<Number>> {

    public static final String NAME = "collection_avg";
    private final FunctionInfo info;

    public static void register(ScalarFunctionModule mod) {
        for (DataType t : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            mod.register(
                new CollectionAverageFunction(
                    new FunctionInfo(new FunctionIdent(
                        NAME, ImmutableList.<DataType>of(new SetType(t))), DataTypes.DOUBLE))
            );
        }
    }

    private CollectionAverageFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public Double evaluate(Input<Set<Number>>... args) {
        // NOTE: always returning double ignoring the input type, maybe better implement type safe
        Set<Number> arg0Value = args[0].value();
        if (arg0Value == null) {
            return null;
        }
        double sum = 0;
        long count = 0;
        for (Number value : arg0Value) {
            sum += value.doubleValue();
            count++;
        }
        if (count > 0) {
            return sum / count;
        } else {
            return null;
        }
    }

    @Override
    public FunctionInfo info() {
        return info;
    }
}
