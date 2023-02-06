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

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class Pattern<T> {

    private static final DefaultMatcher DEFAULT_MATCHER = new DefaultMatcher();

    public static <T> Pattern<T> typeOf(Class<T> expectedClass) {
        return new TypeOfPattern<>(expectedClass);
    }

    public <U, V> Pattern<T> with(Function<? super T, Optional<U>> getProperty, Pattern<V> propertyPattern) {
        return new WithPattern<>(this, getProperty, propertyPattern);
    }

    public Pattern<T> with(Predicate<? super T> propertyPredicate) {
        return new WithPropertyPattern<>(this, propertyPredicate);
    }

    public Pattern<T> capturedAs(Capture<T> capture) {
        return new CapturePattern<>(capture, this);
    }

    abstract Match<T> accept(Matcher matcher, Object object, Captures captures);

    public Match<T> match(Object object, Captures captures) {
        return DEFAULT_MATCHER.match(this, object, captures);
    }

}
