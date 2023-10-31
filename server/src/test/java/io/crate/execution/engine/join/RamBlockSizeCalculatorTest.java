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

package io.crate.execution.engine.join;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.junit.Test;

public class RamBlockSizeCalculatorTest {

    private final CircuitBreaker circuitBreaker = mock(CircuitBreaker.class);
    private int defaultBlockSize = 500_000;

    @Test
    public void testCalculationOfBlockSize() {
        when(circuitBreaker.getLimit()).thenReturn(110L);
        when(circuitBreaker.getUsed()).thenReturn(10L);
        RamBlockSizeCalculator blockCalculator100leftRows = new RamBlockSizeCalculator(
            defaultBlockSize,
            circuitBreaker,
            5
        );
        assertThat(blockCalculator100leftRows.applyAsInt(-1), is(20));
        RamBlockSizeCalculator blockCalculator10LeftRows = new RamBlockSizeCalculator(
            defaultBlockSize,
            circuitBreaker,
            5
        );
        assertThat(blockCalculator10LeftRows.applyAsInt(-1), is(20));
        assertThat(blockCalculator10LeftRows.applyAsInt(50), is(2));
    }

    @Test
    public void testCalculationOfBlockSizeWithMissingStats() {
        when(circuitBreaker.getLimit()).thenReturn(-1L);
        RamBlockSizeCalculator blockSizeCalculator = new RamBlockSizeCalculator(
            defaultBlockSize,
            circuitBreaker,
            10
        );
        assertThat(blockSizeCalculator.applyAsInt(-1), is(RamBlockSizeCalculator.FALLBACK_SIZE));

        when(circuitBreaker.getLimit()).thenReturn(110L);
        when(circuitBreaker.getUsed()).thenReturn(10L);
        RamBlockSizeCalculator blockCalculatorNoNumberOrRowsStats = new RamBlockSizeCalculator(
            defaultBlockSize,
            circuitBreaker,
            10
        );
        assertThat(blockCalculatorNoNumberOrRowsStats.applyAsInt(-1), is(10));

        RamBlockSizeCalculator blockCalculatorNoRowSizeStats = new RamBlockSizeCalculator(
            defaultBlockSize,
            circuitBreaker,
            -1
        );
        assertThat(blockCalculatorNoRowSizeStats.applyAsInt(-1), is(RamBlockSizeCalculator.FALLBACK_SIZE));
    }

    @Test
    public void testCalculationOfBlockSizeWithNoMemLeft() {
        when(circuitBreaker.getLimit()).thenReturn(110L);
        when(circuitBreaker.getUsed()).thenReturn(110L);
        RamBlockSizeCalculator blockSizeCalculator = new RamBlockSizeCalculator(
            defaultBlockSize,
            circuitBreaker,
            10
        );
        assertThat(blockSizeCalculator.applyAsInt(-1), is(10));
    }

    @Test
    public void testCalculationOfBlockSizeWithIntegerOverflow() {
        when(circuitBreaker.getLimit()).thenReturn(Integer.MAX_VALUE + 1L);
        when(circuitBreaker.getUsed()).thenReturn(0L);
        RamBlockSizeCalculator blockSizeCalculator = new RamBlockSizeCalculator(
            defaultBlockSize,
            circuitBreaker,
            1
        );
        assertThat(blockSizeCalculator.applyAsInt(-1), is(500_000));
    }

    @Test
    public void testBlockSizeIsNotGreaterThanPageSize() {
        when(circuitBreaker.getLimit()).thenReturn(defaultBlockSize * 2L);
        when(circuitBreaker.getUsed()).thenReturn(0L);
        RamBlockSizeCalculator blockSizeCalculator = new RamBlockSizeCalculator(
            defaultBlockSize,
            circuitBreaker,
            1
        );
        assertThat(blockSizeCalculator.applyAsInt(-1), is(defaultBlockSize));
    }
}
