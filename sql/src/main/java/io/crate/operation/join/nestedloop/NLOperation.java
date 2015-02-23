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

import io.crate.executor.IteratorPage;
import io.crate.executor.Page;
import rx.Observable;
import rx.Subscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class NLOperation {

    private final NestedLoopOnSubscribe nestedLoopOnSubscribe;

    public NLOperation(Observable<Page> outerPageObservable, Observable<Page> innerPageObservable, int limit) {
        nestedLoopOnSubscribe = new NestedLoopOnSubscribe(outerPageObservable, innerPageObservable, limit);
    }

    public Observable<Page> execute() {
        return Observable.create(nestedLoopOnSubscribe);
    }

    private static class NestedLoopOnSubscribe implements Observable.OnSubscribe<Page> {

        private final Observable<Page> outerPageObservable;
        private final Observable<Page> innerPageObservable;
        private final int limit;

        public NestedLoopOnSubscribe(Observable<Page> outerPageObservable, Observable<Page> innerPageObservable, int limit) {
            this.outerPageObservable = outerPageObservable;
            this.innerPageObservable = innerPageObservable;
            this.limit = limit;
        }

        @Override
        public void call(final Subscriber<? super Page> subscriber) {
            final AtomicBoolean fetchedAllOuter = new AtomicBoolean(false);
            final AtomicBoolean fetchedAllInner = new AtomicBoolean(false);
            final List<Page> innerPages = new ArrayList<>();
            final AtomicInteger rowsProduced = new AtomicInteger(0);

            outerPageObservable.subscribe(new Subscriber<Page>() {

                private void sendLastPageAndComplete(List<Object[]> rows) {
                    subscriber.onNext(new IteratorPage(rows, rows.size(), true));
                    subscriber.onCompleted();;
                }

                private Page toPage(List<Object[]> rows) {
                    return new IteratorPage(rows, rows.size(), false);
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

                @Override
                public void onNext(final Page outerPage) {
                    final List<Object[]> rows = new ArrayList<>();
                    for (final Object[] outerRow : outerPage) {
                        if (fetchedAllInner.get()) {
                            for (Page innerPage : innerPages) {
                                consumeInnerPage(innerPage, rows, outerRow);
                            }
                        } else {
                            final Subscriber<Page> outerSubscriber = this;
                            innerPageObservable.subscribe(new Subscriber<Page>() {
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

                                @Override
                                public void onNext(Page innerPage) {
                                    if (subscriber.isUnsubscribed()) {
                                        unsubscribe();
                                        return;
                                    }

                                    innerPages.add(innerPage);
                                    consumeInnerPage(innerPage, rows, outerRow);
                                }
                            });
                        }
                    }
                    if (!subscriber.isUnsubscribed()) {
                        subscriber.onNext(toPage(rows));
                    } else {
                        unsubscribe();
                    }
                }

                private void consumeInnerPage(Page innerPage, List<Object[]> rows, Object[] outerRow) {
                    for (Object[] innerRow : innerPage) {
                        rows.add(combineRow(innerRow, outerRow));

                        if (rowsProduced.incrementAndGet() >= limit) {
                            sendLastPageAndComplete(rows);
                            return;
                        }
                    }
                }
            });
        }
    }

    private static Object[] combineRow(Object[] innerRow, Object[] outerRow) {
        Object[] newRow = new Object[outerRow.length + innerRow.length];
        System.arraycopy(outerRow, 0, newRow, 0, outerRow.length);
        System.arraycopy(innerRow, 0, newRow, outerRow.length, innerRow.length);
        return newRow;
    }
}
