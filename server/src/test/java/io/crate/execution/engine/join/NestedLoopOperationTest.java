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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.junit.Test;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.data.join.CrossJoinBlockNLBatchIterator;
import io.crate.data.join.CrossJoinNLBatchIterator;
import io.crate.sql.tree.JoinType;

public class NestedLoopOperationTest {

    @Test
    public void testCrossJoinWithBlockNestedLoopIfSingleNodeExecution() {
        BatchIterator<Row> singleNodeLeftSideSmaller = createBatchIterator(true);
        assertThat(singleNodeLeftSideSmaller).isExactlyInstanceOf(CrossJoinBlockNLBatchIterator.class);
    }

    @Test
    public void testCrossJoinWithNestedLoopIfMultipleNodes() {
        BatchIterator<Row> singleNodeLeftSideSmaller = createBatchIterator(false);
        assertThat(singleNodeLeftSideSmaller).isExactlyInstanceOf(CrossJoinNLBatchIterator.class);
    }


    private static BatchIterator<Row> createBatchIterator(boolean blockNlPossible) {
        NoopCircuitBreaker noopCircuitBreaker = new NoopCircuitBreaker("test");
        return NestedLoopOperation.createNestedLoopIterator(
            null,
            2,
            null,
            3,
            JoinType.CROSS,
            row -> false,
            noopCircuitBreaker,
            RamAccounting.NO_ACCOUNTING,
            Collections.emptyList(),
            64L,
            9L,
            blockNlPossible);
    }

}
