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

import java.util.concurrent.atomic.AtomicInteger;

public class BatchedNestedLoopOperation implements CompletionListenable {


    private final SettableFuture<?> completionFuture = SettableFuture.create();
    private final BatchConsumer downstream;
    private BatchCursor leftCursor;
    private BatchCursor rightCursor;
    private final AtomicInteger cursorsReceived = new AtomicInteger();

    private final BatchConsumer left = new BatchConsumer() {
        @Override
        public void accept(BatchCursor batchCursor) {
            leftCursor = batchCursor;
            onCursorReceived();
        }

        @Override
        public boolean requiresScroll() {
            return downstream.requiresScroll();
        }
    };

    private final BatchConsumer right = new BatchConsumer() {
        @Override
        public void accept(BatchCursor batchCursor) {
            rightCursor = batchCursor;
            onCursorReceived();
        }

        @Override
        public boolean requiresScroll() {
            return true;
        }
    };

    private void onCursorReceived() {
        if (cursorsReceived.incrementAndGet() == 2) {
            emit();
        }
        assert cursorsReceived.get() <= 2 : "received more than 2 cursors";
    }

    public BatchConsumer left() {
        return left;
    }

    public BatchConsumer right() {
        return right;
    }

    private void emit() {
        downstream.accept(new OutputCursor());
    }

    public BatchedNestedLoopOperation(BatchConsumer downstream) {
        this.downstream = downstream;
    }

    @Override
    public ListenableFuture<?> completionFuture() {
        return completionFuture;
    }

    class OutputCursor implements BatchCursor {

        private boolean inner = true;
        private volatile Status status = Status.ON_ROW;
        private BatchCursor cursorToMove;
        private int size = leftCursor.size() + rightCursor.size();


        @Override
        public boolean moveFirst() {
            inner = true;
            return leftCursor.moveFirst() && rightCursor.moveFirst();
        }

        @Override
        public boolean moveNext() {
            boolean rightHasData = rightCursor.moveNext();
            System.err.println("rightHasData = " + rightHasData + " inner=" + inner);

            if (rightHasData){
                inner = true;
                return true;
            } else {
                System.err.println("right.allLoaded = " + rightCursor.allLoaded());
                if (rightCursor.allLoaded()){
                    inner = false;
                    boolean leftHasData = leftCursor.moveNext();
                    System.err.println("leftHasData = " + rightHasData);
                    if (leftHasData){
                        rightCursor.moveFirst();
                        return true;
                    } else {
                        System.err.println("left.allLoaded = " + leftCursor.allLoaded());
                        if (!leftCursor.allLoaded()){
                            // more data to come, so move the right cursor already to the beginning
                            rightCursor.moveFirst();
                        }
                        return false;
                    }
                } else {
                    // need to load right
                    inner = true;
                    return false;
                }
            }
        }

        @Override
        public void close() {
            leftCursor.close();
            rightCursor.close();
            completionFuture.set(null);
        }

        @Override
        public Status status() {
            if (leftCursor.status() == Status.ON_ROW) {
                return rightCursor.status();
            } else {
                return leftCursor.status();
            }
        }

        @Override
        public ListenableFuture<?> loadNextBatch() {
            assert !allLoaded(): "all data already loaded";
            System.err.println("loadNextBatch inner=" + inner);
            if (inner) {
                return rightCursor.loadNextBatch();
            } else {
                return leftCursor.loadNextBatch();
            }
        }

        @Override
        public boolean allLoaded() {
            return leftCursor.allLoaded() && rightCursor.allLoaded();
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public Object get(int index) {
            if (index >= leftCursor.size()){
                return rightCursor.get(index - leftCursor.size());
            }
            return leftCursor.get(index);
        }

        @Override
        public Object[] materialize() {
            Object[] a = new Object[size()];
            for (int i = 0; i < a.length; i++) {
                a[i] = get(i);
            }
            return a;
        }
    }
}
