/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.logging;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.SuppressLoggerChecks;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists;
import io.crate.common.collections.RingBuffer;

/**
 * A logger that logs deprecation notices.
 */
public class DeprecationLogger {

    private static final ThreadLocal<RingBuffer<String>> RECENT_WARNINGS = ThreadLocal.withInitial(() -> new RingBuffer<String>(20));

    private final Logger logger;

    /**
     * Creates a new deprecation logger based on the parent logger. Automatically
     * prefixes the logger name with "deprecation", if it starts with "org.elasticsearch.",
     * it replaces "org.elasticsearch" with "org.elasticsearch.deprecation" to maintain
     * the "org.elasticsearch" namespace.
     */
    public DeprecationLogger(Logger parentLogger) {
        String name = parentLogger.getName();
        if (name.startsWith("org.elasticsearch")) {
            name = name.replace("org.elasticsearch.", "org.elasticsearch.deprecation.");
        } else {
            name = "deprecation." + name;
        }
        this.logger = LogManager.getLogger(name);
    }

    // LRU set of keys used to determine if a deprecation message should be emitted to the deprecation logs
    private final Set<String> keys = Collections.newSetFromMap(Collections.synchronizedMap(new LinkedHashMap<String, Boolean>() {
        @Override
        protected boolean removeEldestEntry(final Map.Entry<String, Boolean> eldest) {
            return size() > 128;
        }
    }));

    @VisibleForTesting
    public void resetLRU() {
        keys.clear();
    }

    /**
     * Adds a formatted warning message as a response header on the thread context, and logs a deprecation message if the associated key has
     * not recently been seen.
     *
     * @param key    the key used to determine if this deprecation should be logged
     * @param msg    the message to log
     * @param params parameters to the message
     */
    public void deprecatedAndMaybeLog(final String key, final String msg, final Object... params) {
        deprecated(msg, keys.add(key), params);
    }

    @SuppressLoggerChecks(reason = "safely delegates to logger")
    void deprecated(final String message, final boolean shouldLog, final Object... params) {
        if (shouldLog) {
            logger.warn(message, params);
            var msg = LoggerMessageFormat.format(message, params);
            RECENT_WARNINGS.get().add(msg);
        }
    }

    public static List<String> getRecentWarnings() {
        return Lists.of(RECENT_WARNINGS.get());
    }

    public static void resetWarnings() {
        RECENT_WARNINGS.get().reset();
    }
}
