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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.core.collections.Row;

import javax.annotation.ParametersAreNonnullByDefault;

public class RowReceivers {

    public static ListenableRowReceiver listenableRowReceiver(RowReceiver rowReceiver) {
        if (rowReceiver instanceof ListenableRowReceiver) {
            return (ListenableRowReceiver) rowReceiver;
        }
        return new SettableFutureRowReceiver(rowReceiver);
    }

    @ParametersAreNonnullByDefault
    private static class SettableFutureRowReceiver extends ForwardingRowReceiver implements ListenableRowReceiver {

        private final SettableFuture<Void> finishedFuture = SettableFuture.create();

        SettableFutureRowReceiver(RowReceiver rowReceiver) {
            super(rowReceiver);
        }

        @Override
        public void finish(RepeatHandle repeatHandle) {
            super.finish(repeatHandle);
            finishedFuture.set(null);
        }

        @Override
        public void fail(Throwable throwable) {
            super.fail(throwable);
            finishedFuture.setException(throwable);
        }

        @Override
        public ListenableFuture<Void> finishFuture() {
            return finishedFuture;
        }
    }

    /**
     * Sends exactly one row to the given receiver and calls {@link RowReceiver#finish}. The repeatHandle passed to
     * the receiver does not support repeat.
     *
     * Pause is not supported, so if the receiver tries to pause it {@link RowReceiver#fail} will be called immediately.
     *
     * @param receiver the downstream receiver to send the row to
     * @param row the row to be sent to the receiver
     */
    public static void sendOneRow(RowReceiver receiver, Row row) {
        RowReceiver.Result result = receiver.setNextRow(row);
        if (result == RowReceiver.Result.PAUSE){
            receiver.fail(new UnsupportedOperationException("Pause not supported receiver=" + receiver));
        }
        receiver.finish(RepeatHandle.UNSUPPORTED);
    }
}
