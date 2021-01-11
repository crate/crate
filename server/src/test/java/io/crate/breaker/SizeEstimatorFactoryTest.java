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

package io.crate.breaker;

import io.crate.data.RowN;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.RowType;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SizeEstimatorFactoryTest {

    @Test
    public void testSizeEstimationForArrayType() throws Exception {
        ArrayType<Integer> arrayType = new ArrayType<>(DataTypes.INTEGER);
        SizeEstimator<Object> estimator = SizeEstimatorFactory.create(arrayType);
        assertThat(estimator.estimateSize(List.of(10, 20, 30)), is(64L));
    }

    @Test
    public void testSizeEstimationForObjects() throws Exception {
        SizeEstimator<Object> estimator = SizeEstimatorFactory.create(DataTypes.UNTYPED_OBJECT);
        assertThat(estimator.estimateSize(Collections.emptyMap()), is(24L));
    }

    @Test
    public void testSizeEstimationForGeoPoint() throws Exception {
        SizeEstimator<Object> estimator = SizeEstimatorFactory.create(DataTypes.GEO_POINT);
        assertThat(estimator.estimateSize(new Double[]{0.0d, 0.0d}), is(40L));
    }

    @Test
    public void testSizeEstimationForGeoShape() throws Exception {
        SizeEstimator<Object> estimator = SizeEstimatorFactory.create(DataTypes.GEO_SHAPE);
        assertThat(estimator.estimateSize(Collections.emptyMap()), is(24L));
    }

    @Test
    public void test_estimate_size_of_record() throws Exception {
        var estimator = SizeEstimatorFactory.create(new RowType(List.of(DataTypes.LONG, DataTypes.INTEGER)));
        assertThat(estimator.estimateSize(new RowN(20L, 10)), is(40L));
    }

    @Test
    public void test_create_size_estimator_for_numeric_type() {
        var estimator = SizeEstimatorFactory.create(DataTypes.NUMERIC);
        assertThat(estimator, instanceOf(NumericSizeEstimator.class));
    }
}
