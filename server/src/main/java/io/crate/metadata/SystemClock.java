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

package io.crate.metadata;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

public final class SystemClock {

    private static final AtomicReference<Clock> CURRENT_CLOCK = new AtomicReference<>(Clock.systemUTC());

    public static void setClock(Clock newClock) {
        CURRENT_CLOCK.set(newClock);
    }

    public static void setCurrentMillisFixedUTC(long millis) {
        setClock(Clock.fixed(Instant.ofEpochMilli(millis), ZoneOffset.UTC));
    }

    public static void setCurrentMillisSystemUTC() {
        setClock(Clock.systemUTC());
    }

    public static Instant currentInstant() {
        return CURRENT_CLOCK.get().instant();
    }
}
