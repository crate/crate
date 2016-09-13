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

import com.carrotsearch.hppc.DoubleArrayList;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.util.Set;

/**
 * @see io.crate.operation.scalar.stats.PercentileFunction function estimates percentiles based on sample data, using
 * @see org.apache.commons.math3.stat.descriptive.rank.Percentile from (Apache Commons Math 3.0 API).
 *
 *  The function calculates a single percentile or multiple percntiles:
 *
 *   percentile_cont(fractions, values) -> double[]
 *      returns an array of estimated percentile values, the size of output
 *      the array equals of the size of the `fractions` array.
 *
 *   percentile_cont(fraction, values) -> double
 *   returns a percentile value corresponding to the specified `fraction` parameter.
 *
 *  The percentile function ignore null values in their sorted input.
 *  The fraction value must be a double precision value between 0 and 1.
 */
public abstract class PercentileFunction<R, T> extends Scalar<R, T> {

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

    private PercentileFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }


    protected static double[] toDoubleArray(Object[] array) {
        DoubleArrayList values = new DoubleArrayList(array.length);
        for (Object value : array) {
            if (value != null) {
                values.add(DataTypes.DOUBLE.value(value));
            }
        }
        return values.toArray();
    }

    protected static boolean nullOrEmptyArgs(Object arrayValue, Object fractionValue) {
        return arrayValue == null || fractionValue == null || ((Object[]) arrayValue).length == 0;
    }

    private static class PercentileContArray extends PercentileFunction<Double[], Number[]> {

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
        public Double[] evaluate(Input<Number[]>[] args) {
            assert args.length == 2 : "percentile_cont: requires two arguments" ;
            Object arrayValue = args[1].value();
            Object fractionsValue = args[0].value();

            if (nullOrEmptyArgs(arrayValue, fractionsValue)) {
                return null;
            }

            return evaluatePercentiles((Object[]) arrayValue, (Object[]) fractionsValue);
        }

        private Double[] evaluatePercentiles(Object[] values, Object[] fractions) {
            final Percentile percentile = new Percentile();
            percentile.setData(toDoubleArray((values)));

            Double[] result = new Double[fractions.length];
            for (int idx = 0; idx < fractions.length; idx++) {
                result[idx] = percentile.evaluate(DataTypes.DOUBLE.value(fractions[idx]) * 100.0);
            }
            return result;
        }
    }


    private static class PercentileCont extends PercentileFunction<Double, Object> {

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
        public Double evaluate(Input<Object>[] args) {
            assert args.length == 2 : "percentile_cont: requires two arguments" ;
            Object arrayValue = args[1].value();
            Double fraction = (Double) args[0].value();

            if (nullOrEmptyArgs(arrayValue, fraction)) {
                return null;
            }

            final Percentile percentile = new Percentile();
            return percentile.evaluate(toDoubleArray((Object[]) arrayValue), DataTypes.DOUBLE.value(fraction) * 100);
        }
    }
}
