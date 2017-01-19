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

package io.crate.operation.data;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.concurrent.CompletionListenable;

public class ListenableBatchConsumer implements BatchConsumer, CompletionListenable {

    SettableFuture<Void> completionFuture = SettableFuture.create();
    private final BatchConsumer downstream;

    public ListenableBatchConsumer(BatchConsumer downstream) {
        this.downstream = downstream;
    }

    @Override
    public void accept(BatchCursor batchCursor, Throwable t) {
        if (batchCursor != null) {
            batchCursor = new Cursor(batchCursor);
        } else {
            assert t != null: "Throwable is not set but cursor is null";
            completionFuture.setException(t);
        }
        downstream.accept(batchCursor, t);
    }

    @Override
    public ListenableFuture<?> completionFuture() {
        return completionFuture;
    }

    private class Cursor extends BatchCursorProxy {

        Cursor(BatchCursor delegate) {
            super(delegate);
        }

        @Override
        public void close() {
            super.close();
            completionFuture.set(null);
        }
    }

}
