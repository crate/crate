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

package org.elasticsearch.threadpool;

import java.util.List;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;

/**
 * Base class for executor builders.
 */
public abstract class ExecutorBuilder {

    private final String name;

    public ExecutorBuilder(String name) {
        this.name = name;
    }

    protected String name() {
        return name;
    }

    protected static String settingsKey(final String prefix, final String key) {
        return String.join(".", prefix, key);
    }

    protected int applyHardSizeLimit(final Settings settings, final String name) {
        if (name.equals(ThreadPool.Names.WRITE)) {
            return 1 + EsExecutors.numberOfProcessors(settings);
        } else {
            return Integer.MAX_VALUE;
        }
    }

    /**
     * The list of settings this builder will register.
     *
     * @return the list of registered settings
     */
    public abstract List<Setting<?>> getRegisteredSettings();

    /**
     * Builds the executor with the specified executor settings.
     *
     * @param settings      the executor settings
     * @return a new executor built from the specified executor settings
     */
    abstract ThreadPool.ExecutorHolder build(Settings settings);

    /**
     * Format the thread pool info object for this executor.
     *
     * @param info the thread pool info object to format
     * @return a formatted thread pool info (useful for logging)
     */
    abstract String formatInfo(ThreadPool.Info info);
}
