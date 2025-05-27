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

package io.crate.concurrent;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.elasticsearch.action.ActionListener;

public final class MultiActionListener<I, S, R> implements ActionListener<I> {

    private final AtomicInteger counter;
    private final AtomicReference<Exception> lastExceptions = new AtomicReference<>(null);
    private final BiConsumer<S, I> accumulator;
    private final Function<S, R> finisher;
    private final ActionListener<? super R> actionListener;
    private final S state;

    public static <I> ActionListener<I> of(int numResponses, ActionListener<Collection<I>> listener) {
        return new MultiActionListener<>(numResponses, Collectors.toList(), listener);
    }

    public MultiActionListener(int numResponses, Collector<I, S, R> collector, ActionListener<? super R> listener) {
        this(numResponses, collector.supplier(), collector.accumulator(), collector.finisher(), listener);
    }

    public MultiActionListener(int numResponses,
                               Supplier<S> stateSupplier,
                               BiConsumer<S, I> accumulator,
                               Function<S, R> finisher,
                               ActionListener<? super R> actionListener) {
        this.accumulator = accumulator;
        this.finisher = finisher;
        this.actionListener = actionListener;
        this.state = stateSupplier.get();
        counter = new AtomicInteger(numResponses);
        if (numResponses == 0) {
            actionListener.onResponse(finisher.apply(state));
        }
    }

    @Override
    public void onResponse(I response) {
        synchronized (state) {
            try {
                accumulator.accept(state, response);
            } catch (Exception e) {
                lastExceptions.set(e);
            }
        }
        countdown();
    }

    @Override
    public void onFailure(Exception e) {
        lastExceptions.set(e);
        countdown();
    }

    private void countdown() {
        if (counter.decrementAndGet() == 0) {
            Exception e = lastExceptions.get();
            if (e == null) {
                actionListener.onResponse(finisher.apply(state));
            } else {
                actionListener.onFailure(e);
            }
        }
    }
}
