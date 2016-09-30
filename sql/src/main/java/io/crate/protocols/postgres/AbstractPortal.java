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

package io.crate.protocols.postgres;

import io.crate.action.sql.SQLOperations;
import io.crate.analyze.Analyzer;
import io.crate.executor.Executor;

import java.util.Set;

abstract class AbstractPortal implements Portal {

    protected final String name;
    final SessionData sessionData;

    AbstractPortal(String name,
                   String defaultSchema,
                   Set<SQLOperations.Option> options,
                   Analyzer analyzer,
                   Executor executor,
                   boolean isReadOnly) {
        this.name = name;
        sessionData = new SessionData(defaultSchema, options, analyzer, executor, isReadOnly);
    }

    AbstractPortal(String name, SessionData sessionData) {
        this.name = name;
        this.sessionData = sessionData;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void close() {
    }

    @Override
    public String toString() {
        return "name: " + name + ", type: " + getClass().getSimpleName();
    }

    static class SessionData {

        private Set<SQLOperations.Option> options;
        private final Analyzer analyzer;
        private final Executor executor;
        private final String defaultSchema;
        private final boolean isReadOnly;

        private SessionData(String defaultSchema,
                            Set<SQLOperations.Option> options,
                            Analyzer analyzer,
                            Executor executor,
                            boolean isReadOnly) {
            this.defaultSchema = defaultSchema;
            this.options = options;
            this.analyzer = analyzer;
            this.executor = executor;
            this.isReadOnly = isReadOnly;
        }

        Analyzer getAnalyzer() {
            return analyzer;
        }

        Executor getExecutor() {
            return executor;
        }

        String getDefaultSchema() {
            return defaultSchema;
        }

        Set<SQLOperations.Option> options() {
            return options;
        }

        boolean isReadOnly() {
            return isReadOnly;
        }
    }
}
