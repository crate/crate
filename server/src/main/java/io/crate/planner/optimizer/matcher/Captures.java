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

public class Captures {

    private static final Captures NIL = new Captures(null, null, null);

    private final Capture<?> capture;
    private final Object value;
    private final Captures tail;

    private Captures(Capture<?> capture, Object value, Captures tail) {
        this.capture = capture;
        this.value = value;
        this.tail = tail;
    }

    public static Captures empty() {
        return NIL;
    }

    public static <T> Captures of(Capture<T> capture, T value) {
        return new Captures(capture, value, NIL);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(Capture<T> capture) {
        if (this.equals(NIL)) {
            throw new NoSuchElementException("Requested value for unknown Capture");
        } else if (this.capture.equals(capture)) {
            return (T) value;
        } else {
            return tail.get(capture);
        }
    }

    public Captures add(Captures other) {
        if (this.equals(NIL)) {
            return other;
        } else {
            return new Captures(capture, value, tail.add(other));
        }
    }
}
