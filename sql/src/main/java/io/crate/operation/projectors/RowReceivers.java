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

import io.crate.data.Row;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.concurrent.CompletableFuture;

public class RowReceivers {

    public static ListenableRowReceiver listenableRowReceiver(RowReceiver rowReceiver) {
        if (rowReceiver instanceof ListenableRowReceiver) {
            return (ListenableRowReceiver) rowReceiver;
        }
        return new SettableFutureRowReceiver(rowReceiver);
    }

    @ParametersAreNonnullByDefault
    private static class SettableFutureRowReceiver extends ForwardingRowReceiver implements ListenableRowReceiver {

        private final CompletableFuture<Void> finishedFuture = new CompletableFuture<>();

        SettableFutureRowReceiver(RowReceiver rowReceiver) {
            super(rowReceiver);
        }

        @Override
        public void finish(RepeatHandle repeatHandle) {
            super.finish(repeatHandle);
            finishedFuture.complete(null);
        }

        @Override
        public void fail(Throwable throwable) {
            super.fail(throwable);
            finishedFuture.completeExceptionally(throwable);
        }

        @Override
        public CompletableFuture<?> finishFuture() {
            return finishedFuture;
        }
    }

    public static void sendOneRow(RowReceiver receiver, Row row) {
        receiver.setNextRow(row);
        receiver.finish(RepeatHandle.UNSUPPORTED);
    }

    public static void sendOneRow(RowReceiver receiver, Row row, Throwable t) {
        if (t != null) {
            receiver.fail(t);
        } else {
            sendOneRow(receiver, row);
        }
    }
}
