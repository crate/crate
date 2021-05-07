/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.breaker;

import org.elasticsearch.test.ESTestCase;
import io.crate.testing.RowGenerator;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.MemoryCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.junit.Test;

import java.util.Collections;
import java.util.stream.IntStream;

public class RowAccountingWithEstimatorsTest extends ESTestCase {

    @Test
    public void testCircuitBreakingWorks() throws Exception {
        RowAccountingWithEstimators rowAccounting = new RowAccountingWithEstimators(
            Collections.singletonList(DataTypes.INTEGER),
            ConcurrentRamAccounting.forCircuitBreaker(
                "test",
                new MemoryCircuitBreaker(
                    new ByteSizeValue(10, ByteSizeUnit.BYTES),
                    1.01,
                    LogManager.getLogger(RowAccountingWithEstimatorsTest.class))
            ));

        expectedException.expect(CircuitBreakingException.class);
        RowGenerator.range(0, 3).forEach(rowAccounting::accountForAndMaybeBreak);
    }

    @Test
    public void testRowCellsAccountingCircuitBreakingWorks() throws Exception {
        RowCellsAccountingWithEstimators rowAccounting = new RowCellsAccountingWithEstimators(
            Collections.singletonList(DataTypes.INTEGER),
            ConcurrentRamAccounting.forCircuitBreaker(
                "test",
                new MemoryCircuitBreaker(
                    new ByteSizeValue(10, ByteSizeUnit.BYTES), 1.01, LogManager.getLogger(RowAccountingWithEstimatorsTest.class))
            ), 0);

        expectedException.expect(CircuitBreakingException.class);
        IntStream.range(0, 3).forEach(i -> rowAccounting.accountForAndMaybeBreak(new Object[]{i}));
    }

    @Test
    public void testCircuitBreakingWorksWithExtraSizePerRow() throws Exception {
        RowAccountingWithEstimators rowAccounting = new RowAccountingWithEstimators(
            Collections.singletonList(DataTypes.INTEGER),
            ConcurrentRamAccounting.forCircuitBreaker(
                "test",
                new MemoryCircuitBreaker(
                    new ByteSizeValue(10, ByteSizeUnit.BYTES), 1.01, LogManager.getLogger(RowAccountingWithEstimatorsTest.class))
            ),
            2);

        expectedException.expect(CircuitBreakingException.class);
        RowGenerator.range(0, 2).forEach(rowAccounting::accountForAndMaybeBreak);
    }
}
