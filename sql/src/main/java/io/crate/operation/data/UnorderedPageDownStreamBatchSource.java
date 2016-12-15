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

import com.google.common.util.concurrent.*;
import io.crate.concurrent.CompletionListenable;
import io.crate.core.MultiFutureCallback;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.BucketPage;
import io.crate.core.collections.Row;
import io.crate.operation.PageConsumeListener;
import io.crate.operation.PageDownstream;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class UnorderedPageDownStreamBatchSource implements PageDownstream, CompletionListenable {

    private final int bucketsPerPage;
    private final BatchConsumer downstream;
    private PageCursor cursor;
    private final List<List<Bucket>> pages = new ArrayList<>();
    private final SettableFuture<?> completionFuture = SettableFuture.create();
    private PageConsumeListener pageConsumeListener;
    private boolean upstreamFinished = false;

    private final FutureCallback<List<Bucket>> upstreamCallback = new FutureCallback<List<Bucket>>() {
        @Override
        public void onSuccess(@Nullable List<Bucket> result) {
            pages.add(result);
            if (cursor == null){
                // this is the first result, so we need to create the cursor
                cursor = new PageCursor();
                downstream.accept(cursor);
            } else {
                cursor.onNextPage(result);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            completionFuture.setException(t);
        }
    };

    UnorderedPageDownStreamBatchSource(BatchConsumer downstream, int bucketsPerPage) {
        this.bucketsPerPage = bucketsPerPage;
        this.downstream = downstream;
    }

    @Override
    public void nextPage(BucketPage page, PageConsumeListener listener) {
        MultiFutureCallback<Bucket> multiFutureCallback = new MultiFutureCallback<>(page.buckets().size(), upstreamCallback);
        this.pageConsumeListener = listener;
        for (ListenableFuture<Bucket> bucketFuture : page.buckets()) {
            Futures.addCallback(bucketFuture, multiFutureCallback, MoreExecutors.directExecutor());
        }
    }

    @Override
    public void finish() {
        this.upstreamFinished = true;
        if (cursor.status() == BatchCursor.Status.CLOSED){
            completionFuture.set(null);
        }
    }

    @Override
    public void fail(Throwable t) {
        this.completionFuture.setException(t);
    }

    @Override
    public void kill(Throwable t) {
        this.completionFuture.setException(t);
    }

    @Override
    public ListenableFuture<?> completionFuture() {
        return completionFuture;
    }

    class PageCursor implements BatchCursor {

        private int iPage = 0;
        private int iBucket = 0;
        private Iterator<Row> iter;
        private Row row;
        private volatile Status status = Status.OFF_ROW;
        private SettableFuture<?> nextBatchFuture;

        @Override
        public boolean moveFirst() {
            assert !pages.isEmpty() : "no pages upon move";
            assert pages.get(0).size() == bucketsPerPage : "size of first page is wrong";
            iter = pages.get(iPage = 0).get(iBucket = 0).iterator();
            // we can set the status here, if no rows are available, advance will set it to OFF_ROW
            status = Status.ON_ROW;
            return advance();
        }

        private boolean advance() {
            if (iter.hasNext()) {
                row = iter.next();
                return true;
            } else {
                if (iBucket < bucketsPerPage - 1) {
                    iBucket++;
                    iter = pages.get(iPage).get(iBucket).iterator();
                    return advance();
                } else {
                    iBucket = 0;
                    if (iPage < pages.size() - 1) {
                        iPage++;
                        return advance();
                    } else {
                        row = null;
                        status = Status.OFF_ROW;
                        return false;
                    }
                }
            }
        }

        @Override
        public boolean moveNext() {
            return advance();
        }

        @Override
        public void close() {
            row = null;
            pages.clear();
            pageConsumeListener.finish();
        }

        @Override
        public Status status() {
            return status;
        }

        void onNextPage(List<Bucket> buckets) {
            assert nextBatchFuture != null: "batch future not set upon result";
            assert status == Status.LOADING : "not in loading state when receiving a page";
            assert !iter.hasNext(): "not at the end of batch when receiving result";
            assert iPage == pages.size()-2: "cursor not on the last page of previous batch";
            status = Status.ON_ROW;
            iPage = pages.size()-1;
            iBucket = 0;
            iter = pages.get(iPage).get(iBucket).iterator();
            advance();
            nextBatchFuture.set(null);
        }

        @Override
        public ListenableFuture<?> loadNextBatch() throws IllegalStateException {
            this.nextBatchFuture = null;
            if (completionFuture.isDone()){
                // this is only happens if an exception was happening, so we can use the future to forward
                // the exception
                return completionFuture;
            }
            if (status != Status.OFF_ROW){
                throw new IllegalStateException("Expected cursor status OFF_ROW but is " + status);
            }
            status = Status.LOADING;
            nextBatchFuture = SettableFuture.create();
            pageConsumeListener.needMore();
            return nextBatchFuture;

        }

        @Override
        public boolean allLoaded() {
            // This will never be true.
            return upstreamFinished;
        }

        @Override
        public int size() {
            // normal impls would know about its rows without being on a row
            assert status == Status.ON_ROW : "not on a row";
            return row.size();
        }

        @Override
        public Object get(int index) {
            assert status == Status.ON_ROW : "not on a row";
            return row.get(index);
        }

        @Override
        public Object[] materialize() {
            assert status == Status.ON_ROW : "not on a row";
            return row.materialize();
        }
    }
}
