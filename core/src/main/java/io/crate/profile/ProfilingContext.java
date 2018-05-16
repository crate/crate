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

package io.crate.profile;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.function.Function;

/**
 * Simple stop watch type class that can be used as a context across multiple layers (analyzer, planner, executor)
 * to accumulate timing results in a map.
 *
 * It is not meant to be thread-safe.
 *
 */
public class ProfilingContext {

    private final boolean enabled;
    private final ImmutableMap.Builder<String, Long> map;
    private final Function<String, TimerToken> tokenProvider;

    public ProfilingContext(boolean enabled) {
        this.enabled = enabled;
        this.map = ImmutableMap.builder();
        this.tokenProvider = enabled ? InternalTimerToken::new : NoopTimerToken::new;
    }

    public boolean enabled() {
        return enabled;
    }

    public Map<String, Long> getAsMap() {
        return map.build();
    }

    public TimerToken createAndStartTimer(String name) {
        TimerToken timer = createTimer(name);
        timer.start();
        return timer;
    }

    public void stopAndAddTimer(TimerToken token) {
        token.stop();
        if (enabled) {
            map.put(token.name(), token.durationNanos() / 1_000_000L);
        }
    }

    public TimerToken createTimer(String name) {
        return tokenProvider.apply(name);
    }
}
