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

import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.RowGenerator;
import io.crate.types.DataTypes;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.MemoryCircuitBreaker;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class RowAccountingWithEstimatorsTest extends CrateUnitTest {

    private long originalBufferSize;

    @Before
    public void reduceFlushBufferSize() throws Exception {
        originalBufferSize = RamAccountingContext.FLUSH_BUFFER_SIZE;
        RamAccountingContext.FLUSH_BUFFER_SIZE = 20;
    }

    @After
    public void resetFlushBufferSize() throws Exception {
        RamAccountingContext.FLUSH_BUFFER_SIZE = originalBufferSize;
    }

    @Test
    public void testCircuitBreakingWorks() throws Exception {
        RowAccounting rowAccounting = new RowAccountingWithEstimators(Collections.singletonList(DataTypes.INTEGER),
            new RamAccountingContext(
                "test",
                new MemoryCircuitBreaker(
                    new ByteSizeValue(10, ByteSizeUnit.BYTES), 1.01, Loggers.getLogger(RowAccountingWithEstimatorsTest.class))
            ));

        expectedException.expect(CircuitBreakingException.class);
        RowGenerator.range(0, 3).forEach(rowAccounting::accountForAndMaybeBreak);
    }

    @Test
    public void testCircuitBreakingWorksWithExtraSizePerRow() throws Exception {
        RowAccounting rowAccounting = new RowAccountingWithEstimators(Collections.singletonList(DataTypes.INTEGER),
            new RamAccountingContext(
                "test",
                new MemoryCircuitBreaker(
                    new ByteSizeValue(10, ByteSizeUnit.BYTES), 1.01, Loggers.getLogger(RowAccountingWithEstimatorsTest.class))
            ),
            2);

        expectedException.expect(CircuitBreakingException.class);
        RowGenerator.range(0, 2).forEach(rowAccounting::accountForAndMaybeBreak);
    }
}
