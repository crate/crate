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

package io.crate.operation.scalar.stats;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Doubles;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class PercentileFunction<R> extends Scalar<R, Object> {

    private static final String NAME = "percentile_cont";
    private static final DataType DOUBLE_ARRAY_TYPE = new ArrayType(DataTypes.DOUBLE);
    private static final Set<DataType> ALLOWED_TYPES = ImmutableSet.<DataType>builder()
                                                           .addAll(DataTypes.NUMERIC_PRIMITIVE_TYPES)
                                                           .add(DataTypes.UNDEFINED)
                                                           .build();

    public static void register(ScalarFunctionModule module) {
        PercentileContArray.register(module);
        PercentileCont.register(module);
    }

    private final FunctionInfo info;
    private final Percentile percentile;

    private PercentileFunction(FunctionInfo info) {
        this.info = info;
        this.percentile = new Percentile();
    }

    @Override
    public R evaluate(Input[] args) {
        assert args.length == 2;
        if (hasNullInputs(args)) {
            return null;
        }
        Object[] inputArray = (Object[]) args[1].value();
        if (inputArray.length == 0) {
            return null;
        }
        List<Double> values = new ArrayList<>();
        for (Object value : inputArray) {
            if (value != null) {
                values.add(DataTypes.DOUBLE.value(value));
            }
        }
        percentile.setData(Doubles.toArray(values));
        return eval(args[0].value());
    }

    protected abstract R eval(Object percentile);

    @Override
    public FunctionInfo info() {
        return info;
    }

    protected Percentile percentile() {
        return percentile;
    }

    private static class PercentileContArray extends PercentileFunction<Double[]> {

        public static void register(ScalarFunctionModule module) {
            for (DataType dataType : ALLOWED_TYPES) {
                FunctionIdent ident = new FunctionIdent(NAME, ImmutableList.of(DOUBLE_ARRAY_TYPE, new ArrayType(dataType)));
                module.register(new PercentileContArray(new FunctionInfo(ident, DOUBLE_ARRAY_TYPE)));
            }
        }

        PercentileContArray(FunctionInfo info) {
            super(info);
        }

        @Override
        protected Double[] eval(Object fractionsArg) {
            Object[] fractions = (Object[]) fractionsArg;
            Double[] result = new Double[fractions.length];
            for (int idx = 0; idx < fractions.length; idx++) {
                result[idx] = percentile().evaluate(DataTypes.DOUBLE.value(fractions[idx]) * 100.0);
            }
            return result;
        }
    }

    private static class PercentileCont extends PercentileFunction<Double> {

        public static void register(ScalarFunctionModule module) {
            for (DataType dataType : ALLOWED_TYPES) {
                FunctionIdent ident
                    = new FunctionIdent(NAME, ImmutableList.<DataType>of(DataTypes.DOUBLE, new ArrayType(dataType)));
                module.register(new PercentileCont(new FunctionInfo(ident, DataTypes.DOUBLE)));
            }
        }

        PercentileCont(FunctionInfo info) {
            super(info);
        }

        @Override
        protected Double eval(Object fraction) {
            return percentile().evaluate(DataTypes.DOUBLE.value(fraction) * 100);
        }
    }
}
