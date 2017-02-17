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

package io.crate.operation.projectors;

import io.crate.data.BatchIterator;
import io.crate.data.CloseAssertingBatchIterator;
import io.crate.data.Row1;
import io.crate.data.RowsBatchIterator;
import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.CollectingRowReceiver;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class BatchConsumerToRowReceiverTest {

    private BatchIterator iterator;

    @Before
    public void setUp() throws Exception {
        iterator = RowsBatchIterator.newInstance(Arrays.asList(new Row1(1), new Row1(2)));
    }

    @Test
    public void failReceiverIfConsumerFails() {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        BatchConsumerToRowReceiver adapter = new BatchConsumerToRowReceiver(rowReceiver);
        Exception failure = new Exception();

        adapter.accept(iterator, failure);
        assertThat(rowReceiver.completionFuture().isCompletedExceptionally(), is(true));
        assertThat(rowReceiver.getNumFailOrFinishCalls(), is(1));
    }

    @Test
    public void cursorIsClosedAfterAllIsConsumed() {
        RowReceiver rowReceiver = new CollectingRowReceiver();
        BatchConsumerToRowReceiver adapter = new BatchConsumerToRowReceiver(rowReceiver);

        adapter.accept(iterator, null);
        assertIteratorIsClosed();
    }

    private void assertIteratorIsClosed() {
        try {
            iterator.moveNext();
            fail("Iterator should not be allowed to move forward after it's consumed");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage().contains("is closed"), is(true));
        }
    }

    @Test
    public void cursorIsClosedWhenReceiverFails() {
        CollectingRowReceiver failingRowReceiver = CollectingRowReceiver.withFailure();
        BatchConsumerToRowReceiver adapter = new BatchConsumerToRowReceiver(failingRowReceiver);

        adapter.accept(iterator, null);
        assertThat(failingRowReceiver.getNumFailOrFinishCalls(), is(1));
        assertIteratorIsClosed();
    }

    @Test
    public void finishReceiverWhenNextRowYieldsStop() {
        CollectingRowReceiver stoppingRowReceiver = CollectingRowReceiver.withLimit(1);
        BatchConsumerToRowReceiver adapter = new BatchConsumerToRowReceiver(stoppingRowReceiver);

        adapter.accept(iterator, null);
        assertThat(stoppingRowReceiver.completionFuture().isDone(), is(true));
        assertThat(stoppingRowReceiver.getNumFailOrFinishCalls(), is(1));
    }

    @Test
    public void pauseReceiverWhenNextRowYieldsPause() {
        CollectingRowReceiver pausingReceiver = CollectingRowReceiver.withPauseAfter(1);
        BatchConsumerToRowReceiver adapter = new BatchConsumerToRowReceiver(pausingReceiver);
        adapter.accept(iterator, null);

        assertThat(pausingReceiver.numPauseProcessed(), is(1));
    }

    @Test
    public void nextBatchIsLoadedAndConsumed() throws Exception {
        BatchIterator batchIterator = new CloseAssertingBatchIterator(
            new BatchSimulatingIterator(iterator, 2, 1));
        CollectingRowReceiver collectingRowReceiver = new CollectingRowReceiver();
        BatchConsumerToRowReceiver adapter = new BatchConsumerToRowReceiver(collectingRowReceiver);

        adapter.accept(batchIterator, null);

        collectingRowReceiver.completionFuture().get(10, TimeUnit.SECONDS);
        assertThat(collectingRowReceiver.isFinished(), is(true));
    }
}
