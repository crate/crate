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

package io.crate.breaker;

import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class SizeEstimatorTest extends CrateUnitTest {

    @Test
    public void testString() throws Exception {
        SizeEstimator sizeEstimator = SizeEstimatorFactory.create(DataTypes.STRING);
        assertThat(sizeEstimator.estimateSize(null), is(8L));
        assertThat(sizeEstimator.estimateSize("hello"), is(37L));
        assertThat(sizeEstimator.estimateSizeDelta("hello", "hello world"), is(6L));
    }

    @Test
    public void testConstant() throws Exception {
        SizeEstimator sizeEstimator = SizeEstimatorFactory.create(DataTypes.BYTE);
        assertThat(sizeEstimator.estimateSize(null), is(8L));
        assertThat(sizeEstimator.estimateSize(Byte.valueOf("100")), is(16L));
        assertThat(sizeEstimator.estimateSizeDelta(Byte.valueOf("100"), Byte.valueOf("42")), is(0L));
    }

    @Test
    public void testGeoPoint() throws Exception {
        SizeEstimator sizeEstimator = SizeEstimatorFactory.create(DataTypes.GEO_POINT);
        assertThat(sizeEstimator.estimateSize(null), is(8L));
        assertThat(sizeEstimator.estimateSize(new Double[]{1.0d, 2.0d}), is(40L));
        assertThat(sizeEstimator.estimateSizeDelta(new Double[]{1.0d, 2.0d}, null), is(-32L));
    }
}
