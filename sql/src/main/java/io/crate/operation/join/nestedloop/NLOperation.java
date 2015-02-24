/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.join.nestedloop;

import com.google.common.collect.ImmutableList;
import io.crate.executor.IteratorPage;
import io.crate.executor.Page;
import io.crate.operation.projectors.Projector;
import rx.Observable;
import rx.Subscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class NLOperation {

    private final Observable<Page> outerPageObservable;
    private final Observable<Page> innerPageObservable;

    public NLOperation(Observable<Page> outerPageObservable, Observable<Page> innerPageObservable) {
        this.outerPageObservable = outerPageObservable;
        this.innerPageObservable = innerPageObservable;
    }

    public Observable<Page> execute(int offset, int limit, int pageSize) {
        return Observable.create(new NestedLoopOnSubscribe(outerPageObservable, innerPageObservable, offset, limit, pageSize));
    }

    public void execute(final Projector downstream) {
        final AtomicBoolean fetchedAllOuter = new AtomicBoolean(false);
        final AtomicBoolean fetchedAllInner = new AtomicBoolean(false);
        final ProjectorBridge bridge = new ProjectorBridge(downstream);
        final List<Page> innerPages = new ArrayList<>();

        outerPageObservable.subscribe(new OuterSubscriber(bridge, fetchedAllOuter, fetchedAllInner) {

            private boolean sendPageToDownstream(Object[] outerRow, Page innerPage) {
                for (Object[] innerRow : innerPage) {
                    boolean canContinue = downstream.setNextRow(combineRow(innerRow, outerRow));
                    if (!canContinue) {
                        onCompleted();
                        return true;
                    }
                }
                return false;
            }
            @Override
            public void onNext(Page outerPage) {
                for (final Object[] outerRow : outerPage) {
                    if (fetchedAllInner.get()) {
                        for (Page innerPage : innerPages) {
                            if (sendPageToDownstream(outerRow, innerPage)) return;
                        }
                    } else {
                        final Subscriber<Page> outerSubscriber = this;
                        innerPageObservable.subscribe(
                                new InnerSubscriber(bridge, outerSubscriber, fetchedAllOuter, fetchedAllInner) {

                                    @Override
                                    public void onNext(Page innerPage) {
                                        if (sendPageToDownstream(outerRow, innerPage)) return;
                                        innerPages.add(innerPage);
                                    }
                                });
                    }
                }
            }
        });
    }

    private static class NestedLoopOnSubscribe implements Observable.OnSubscribe<Page> {

        private final Observable<Page> outerPageObservable;
        private final Observable<Page> innerPageObservable;
        private final int offset;
        private final int limit;
        private final int pageSize;

        public NestedLoopOnSubscribe(Observable<Page> outerPageObservable,
                                     Observable<Page> innerPageObservable,
                                     int offset,
                                     int limit,
                                     int pageSize) {
            this.outerPageObservable = outerPageObservable;
            this.innerPageObservable = innerPageObservable;
            this.offset = offset;
            this.limit = limit;
            this.pageSize = pageSize;
        }

        @Override
        public void call(final Subscriber<? super Page> subscriber) {
            final AtomicBoolean fetchedAllOuter = new AtomicBoolean(false);
            final AtomicBoolean fetchedAllInner = new AtomicBoolean(false);
            final List<Page> innerPages = new ArrayList<>();
            final AtomicInteger rowsProduced = new AtomicInteger(0);
            final AtomicInteger rowsSkipped = new AtomicInteger(0);
            final List<Object[]> rowBuffer = new ArrayList<>(pageSize);

            outerPageObservable.subscribe(new OuterSubscriber(subscriber, fetchedAllOuter, fetchedAllInner) {

                private void sendLastPageAndComplete(List<Object[]> rows) {
                    subscriber.onNext(new IteratorPage(rows, rows.size(), true));
                    subscriber.onCompleted();
                }

                @Override
                public void onCompleted() {
                    if (fetchedAllInner.get()) {
                        sendLastPageAndComplete(rowBuffer);
                    } else {
                        fetchedAllOuter.set(true);
                    }
                }

                @Override
                public void onNext(final Page outerPage) {
                    for (final Object[] outerRow : outerPage) {
                        if (fetchedAllInner.get()) {
                            for (Page innerPage : innerPages) {
                                consumeInnerPage(innerPage, rowBuffer, outerRow);
                            }
                        } else {
                            final Subscriber<Page> outerSubscriber = this;
                            innerPageObservable.subscribe(
                                    new InnerSubscriber(subscriber, outerSubscriber, fetchedAllOuter, fetchedAllInner) {

                                        @Override
                                        public void onCompleted() {
                                            if (fetchedAllOuter.get()) {
                                                sendLastPageAndComplete(rowBuffer);
                                            } else {
                                                fetchedAllInner.set(true);
                                            }
                                        }

                                        @Override
                                        public void onNext(Page innerPage) {
                                            if (subscriber.isUnsubscribed()) {
                                                unsubscribe();
                                                return;
                                            }

                                            innerPages.add(innerPage);
                                            consumeInnerPage(innerPage, rowBuffer, outerRow);
                                        }
                            });
                        }
                    }
                    if (subscriber.isUnsubscribed()) {
                        unsubscribe();
                    }
                }

                private void consumeInnerPage(Page innerPage, List<Object[]> rows, Object[] outerRow) {
                    for (Object[] innerRow : innerPage) {
                        if (rowsSkipped.getAndIncrement() < offset) {
                            continue;
                        }

                        rows.add(combineRow(innerRow, outerRow));
                        if (rowsProduced.incrementAndGet() >= limit) {
                            sendLastPageAndComplete(rows);
                            return;
                        } else if (rows.size() == pageSize) {
                            subscriber.onNext(new IteratorPage(ImmutableList.copyOf(rows), rows.size(), true));
                            rows.clear();
                        }
                    }
                }
            });
        }
    }

    private static class ProjectorBridge extends Subscriber<Page> {

        private final Projector downstream;

        public ProjectorBridge(Projector downstream) {
            this.downstream = downstream;
        }

        @Override
        public void onCompleted() {
            downstream.upstreamFinished();
        }

        @Override
        public void onError(Throwable e) {
            downstream.upstreamFailed(e);
        }

        @Override
        public void onNext(Page rows) {
            throw new UnsupportedOperationException("onNext not supported on ProjectorBridge.. call setNextRow directly");
        }
    }

    private abstract static class InnerSubscriber extends Subscriber<Page> {

        private final Subscriber<? super Page> subscriber;
        private final Subscriber<? super Page> outerSubscriber;
        private final AtomicBoolean fetchedAllOuter;
        private final AtomicBoolean fetchedAllInner;

        public InnerSubscriber(Subscriber<? super Page> subscriber,
                               Subscriber<? super Page> outerSubscriber,
                               AtomicBoolean fetchedAllOuter,
                               AtomicBoolean fetchedAllInner) {
            this.subscriber = subscriber;
            this.outerSubscriber = outerSubscriber;
            this.fetchedAllOuter = fetchedAllOuter;
            this.fetchedAllInner = fetchedAllInner;
        }

        @Override
        public void onCompleted() {
            if (fetchedAllOuter.get()) {
                subscriber.onCompleted();
            } else {
                fetchedAllInner.set(true);
            }
        }

        @Override
        public void onError(Throwable e) {
            outerSubscriber.onError(e);
        }
    }

    private abstract static class OuterSubscriber extends Subscriber<Page> {

        private final Subscriber<? super Page> subscriber;
        private final AtomicBoolean fetchedAllOuter;
        private final AtomicBoolean fetchedAllInner;

        public OuterSubscriber(Subscriber<? super Page> subscriber,
                               AtomicBoolean fetchedAllOuter,
                               AtomicBoolean fetchedAllInner) {
            this.subscriber = subscriber;
            this.fetchedAllOuter = fetchedAllOuter;
            this.fetchedAllInner = fetchedAllInner;
        }

        @Override
        public void onCompleted() {
            if (fetchedAllInner.get()) {
                subscriber.onCompleted();
            } else {
                fetchedAllOuter.set(true);
            }
        }

        @Override
        public void onError(Throwable e) {
            subscriber.onError(e);
        }
    }

    private static Object[] combineRow(Object[] innerRow, Object[] outerRow) {
        Object[] newRow = new Object[outerRow.length + innerRow.length];
        System.arraycopy(outerRow, 0, newRow, 0, outerRow.length);
        System.arraycopy(innerRow, 0, newRow, outerRow.length, innerRow.length);
        return newRow;
    }
}
