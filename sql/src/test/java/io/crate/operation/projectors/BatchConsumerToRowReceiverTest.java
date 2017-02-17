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
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

public class BatchConsumerToRowReceiverTest {

    private RowReceiver rowReceiver;
    private CompletableFuture rowReceiverCompletionFuture;

    @Before
    public void setUp() throws Exception {
        rowReceiver = mock(RowReceiver.class);
        rowReceiverCompletionFuture = new CompletableFuture<>();
        when(rowReceiver.completionFuture()).thenReturn(rowReceiverCompletionFuture);
    }

    @Test
    public void failReceiverIfConsumerFails() {
        BatchConsumerToRowReceiver adapter = new BatchConsumerToRowReceiver(rowReceiver);

        BatchIterator cursor = mock(BatchIterator.class);
        Exception failure = new Exception();
        adapter.accept(cursor, failure);

        verify(rowReceiver).fail(failure);
    }

    @Test
    public void cursorIsClosedAfterAllIsConsumed() {
        BatchIterator cursor = mock(BatchIterator.class);
        when(cursor.allLoaded()).thenReturn(true);
        when(cursor.moveNext())
            .thenReturn(true)
            .thenReturn(false);

        when(rowReceiver.setNextRow(cursor.currentRow()))
            .thenReturn(RowReceiver.Result.CONTINUE);

        BatchConsumerToRowReceiver adapter = new BatchConsumerToRowReceiver(rowReceiver);
        adapter.accept(cursor, null);

        verify(rowReceiver).setNextRow(cursor.currentRow());
        rowReceiverCompletionFuture.complete(null);
        verify(cursor).close();
    }

    @Test
    public void cursorIsClosedWhenReceiverFails() {
        BatchIterator cursor = mock(BatchIterator.class);
        when(cursor.allLoaded()).thenReturn(true);
        when(cursor.moveNext())
            .thenReturn(true)
            .thenReturn(false);

        when(rowReceiver.setNextRow(cursor.currentRow()))
            .thenReturn(RowReceiver.Result.CONTINUE);

        BatchConsumerToRowReceiver adapter = new BatchConsumerToRowReceiver(rowReceiver);
        adapter.accept(cursor, null);

        rowReceiverCompletionFuture.completeExceptionally(new Exception("RowReceiver failure"));
        verify(cursor).close();
    }

    @Test
    public void finishReceiverAndCloseCursorWhenNextRowYieldsStop() {
        BatchIterator cursor = mock(BatchIterator.class);
        when(cursor.allLoaded()).thenReturn(true);
        when(cursor.moveNext())
            .thenReturn(true)
            .thenReturn(true);

        when(rowReceiver.setNextRow(cursor.currentRow()))
            .thenReturn(RowReceiver.Result.STOP);

        BatchConsumerToRowReceiver adapter = new BatchConsumerToRowReceiver(rowReceiver);
        adapter.accept(cursor, null);

        rowReceiverCompletionFuture.complete(null);
        verify(cursor).close();
        verify(rowReceiver).finish(any());
    }

    @Test
    public void pauseReceiverWhenNextRowYieldsPause() {
        BatchIterator cursor = mock(BatchIterator.class);
        when(cursor.allLoaded()).thenReturn(true);
        when(cursor.moveNext())
            .thenReturn(true)
            .thenReturn(true);

        when(rowReceiver.setNextRow(cursor.currentRow()))
            .thenReturn(RowReceiver.Result.PAUSE);

        BatchConsumerToRowReceiver adapter = new BatchConsumerToRowReceiver(rowReceiver);
        adapter.accept(cursor, null);

        verify(cursor, times(0)).close();
        verify(rowReceiver).pauseProcessed(any(ResumeHandle.class));
    }

    @Test
    public void nextBatchIsLoadedAndConsumed() {
        BatchIterator cursor = mock(BatchIterator.class);
        when(cursor.allLoaded())
            .thenReturn(false)
            .thenReturn(true);

        when(cursor.loadNextBatch()).thenReturn(CompletableFuture.completedFuture(null));

        when(cursor.moveNext())
            .thenReturn(false)
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);

        when(rowReceiver.setNextRow(cursor.currentRow()))
            .thenReturn(RowReceiver.Result.CONTINUE);

        BatchConsumerToRowReceiver adapter = new BatchConsumerToRowReceiver(rowReceiver);
        adapter.accept(cursor, null);

        verify(rowReceiver, times(2)).setNextRow(cursor.currentRow());
    }

    @Test
    public void failReceiverIfLoadingNextBatchFails() {
        BatchIterator cursor = mock(BatchIterator.class);
        when(cursor.allLoaded()).thenReturn(false);
        when(cursor.moveNext())
            .thenReturn(false);

        CompletableFuture failedFuture = new CompletableFuture();
        Exception loadNextBatchException = new Exception();
        failedFuture.completeExceptionally(loadNextBatchException);
        when(cursor.loadNextBatch()).thenReturn(failedFuture);

        BatchConsumerToRowReceiver adapter = new BatchConsumerToRowReceiver(rowReceiver);
        adapter.accept(cursor, null);

        verify(rowReceiver).fail(loadNextBatchException);
    }

}
