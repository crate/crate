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

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Simple stop watch type class that can be used as a context across multiple layers (analyzer, planner, executor)
 * to accumulate timing results in a map.
 *
 * It is not meant to be thread-safe.
 *
 */
public class ProfilingContext {

    private final boolean enabled;
    private final ImmutableMap.Builder<String, Object> map;

    public ProfilingContext(boolean enabled) {
        this.enabled = enabled;
        this.map = ImmutableMap.builder();
    }

    public boolean enabled() {
        return enabled;
    }

    public Map<String, Object> build() {
        return map.build();
    }

    @Nullable
    public TimerToken startTiming(String name) {
        if (enabled) {
            return new TimerToken(name);
        }
        return null;
    }

    public void stopTiming(@Nullable TimerToken token) {
        if (enabled && token != null) {
            long duration = System.nanoTime() - token.startTimeNanos;
            map.put(token.name, duration / 1_000_000L);
        }
    }

    public static class TimerToken {

        private final String name;
        private final long startTimeNanos;

        private TimerToken(String name) {
            this.name = name;
            this.startTimeNanos = System.nanoTime();
        }
    }
}
