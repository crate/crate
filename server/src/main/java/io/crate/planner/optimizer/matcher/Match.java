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

package io.crate.planner.optimizer.matcher;

import java.util.NoSuchElementException;
import java.util.function.Function;

public abstract class Match<T> {

    public static <T> Match<T> of(T value, Captures captures) {
        return new Present<>(value, captures);
    }

    @SuppressWarnings("unchecked")
    public static <T> Match<T> empty() {
        return (Match<T>) Empty.INSTANCE;
    }

    public abstract Captures captures();

    public abstract boolean isPresent();

    public abstract T value();

    public abstract <U> Match<U> map(Function<? super T, ? extends U> mapper);

    public abstract <U> Match<U> flatMap(Function<? super T, Match<U>> mapper);

    static class Present<T> extends Match<T> {

        private final T value;
        private final Captures captures;

        Present(T value, Captures captures) {
            this.value = value;
            this.captures = captures;
        }

        @Override
        public Captures captures() {
            return captures;
        }

        @Override
        public boolean isPresent() {
            return true;
        }

        @Override
        public T value() {
            return value;
        }

        @Override
        public <U> Match<U> map(Function<? super T, ? extends U> mapper) {
            return Match.of(mapper.apply(value), captures);
        }

        @Override
        public <U> Match<U> flatMap(Function<? super T, Match<U>> mapper) {
            return mapper.apply(value);
        }
    }

    static class Empty<T> extends Match<T> {

        @SuppressWarnings("rawtypes")
        static final Empty INSTANCE = new Empty<>();

        @Override
        public Captures captures() {
            return Captures.empty();
        }

        @Override
        public boolean isPresent() {
            return false;
        }

        @Override
        public T value() {
            throw new NoSuchElementException("Empty match contains no value");
        }

        @Override
        public <U> Match<U> map(Function<? super T, ? extends U> mapper) {
            return empty();
        }

        @Override
        public <U> Match<U> flatMap(Function<? super T, Match<U>> mapper) {
            return empty();
        }
    }
}
