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

package io.crate.execution.engine.join;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.junit.Test;

import static io.crate.execution.engine.join.BlockSizeCalculator.DEFAULT_BLOCK_SIZE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BlockSizeCalculatorTest {

    private final CircuitBreaker circuitBreaker = mock(CircuitBreaker.class);

    @Test
    public void testCalculationOfBlockSize() {
        when(circuitBreaker.getLimit()).thenReturn(110L);
        when(circuitBreaker.getUsed()).thenReturn(10L);
        BlockSizeCalculator blockCalculator100leftRows = new BlockSizeCalculator(circuitBreaker, 5, 100);
        assertThat(blockCalculator100leftRows.calculateBlockSize(), is(20));
        BlockSizeCalculator blockCalculator10LeftRows = new BlockSizeCalculator(circuitBreaker, 5, 10);
        assertThat(blockCalculator10LeftRows.calculateBlockSize(), is(10));
    }

    @Test
    public void testCalculationOfBlockSizeWithMissingStats() {
        when(circuitBreaker.getLimit()).thenReturn(-1L);
        BlockSizeCalculator blockSizeCalculator = new BlockSizeCalculator(circuitBreaker, 10, 10);
        assertThat(blockSizeCalculator.calculateBlockSize(), is(DEFAULT_BLOCK_SIZE));

        when(circuitBreaker.getLimit()).thenReturn(110L);
        when(circuitBreaker.getUsed()).thenReturn(10L);
        BlockSizeCalculator blockCalculatorNoNumberOrRowsStats = new BlockSizeCalculator(circuitBreaker, 10, -1);
        assertThat(blockCalculatorNoNumberOrRowsStats.calculateBlockSize(), is(DEFAULT_BLOCK_SIZE));

        BlockSizeCalculator blockCalculatorNoRowSizeStats = new BlockSizeCalculator(circuitBreaker, -1, 10);
        assertThat(blockCalculatorNoRowSizeStats.calculateBlockSize(), is(DEFAULT_BLOCK_SIZE));
    }

    @Test
    public void testCalculationOfBlockSizeWithNoMemLeft() {
        when(circuitBreaker.getLimit()).thenReturn(110L);
        when(circuitBreaker.getUsed()).thenReturn(110L);
        BlockSizeCalculator blockSizeCalculator = new BlockSizeCalculator(circuitBreaker, 10, 10);
        assertThat(blockSizeCalculator.calculateBlockSize(), is(10));
    }

    @Test
    public void testCalculationOfBlockSizeWithIntegerOverflow() {
        when(circuitBreaker.getLimit()).thenReturn(Integer.MAX_VALUE + 1L);
        when(circuitBreaker.getUsed()).thenReturn(0L);
        BlockSizeCalculator blockSizeCalculator = new BlockSizeCalculator(circuitBreaker, 1, 1);
        assertThat(blockSizeCalculator.calculateBlockSize(), is(1));
    }

    @Test
    public void testBlockSizeIsNotGreaterThanPageSize() {
        when(circuitBreaker.getLimit()).thenReturn(DEFAULT_BLOCK_SIZE * 2L);
        when(circuitBreaker.getUsed()).thenReturn(0L);
        BlockSizeCalculator blockSizeCalculator = new BlockSizeCalculator(circuitBreaker, 1, DEFAULT_BLOCK_SIZE * 2L);
        assertThat(blockSizeCalculator.calculateBlockSize(), is(DEFAULT_BLOCK_SIZE));
    }
}
