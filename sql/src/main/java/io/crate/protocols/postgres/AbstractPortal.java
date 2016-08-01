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

import io.crate.action.sql.SessionCtx;
import io.crate.analyze.Analyzer;
import io.crate.executor.Executor;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;

abstract class AbstractPortal implements Portal {

    protected final String name;
    final SessionCtx sessionCtx;
    final SessionData sessionData;

    AbstractPortal(String name,
                   Analyzer analyzer,
                   Executor executor,
                   TransportKillJobsNodeAction transportKillJobsNodeAction,
                   boolean isReadOnly,
                   SessionCtx sessionCtx) {
        this.name = name;
        this.sessionCtx = sessionCtx;
        sessionData = new SessionData(
            analyzer,
            executor,
            transportKillJobsNodeAction,
            isReadOnly
        );
    }

    AbstractPortal(String name, SessionData sessionData, SessionCtx sessionCtx) {
        this.name = name;
        this.sessionData = sessionData;
        this.sessionCtx = sessionCtx;
    }

    @Override
    public void close() {}

    @Override
    public String toString() {
        return "name: " + name + ", type: " + getClass().getSimpleName();
    }

    static class SessionData {

        private final Analyzer analyzer;
        private final Executor executor;
        private final TransportKillJobsNodeAction transportKillJobsNodeAction;
        private final boolean isReadOnly;

        private SessionData(Analyzer analyzer,
                            Executor executor,
                            TransportKillJobsNodeAction transportKillJobsNodeAction,
                            boolean isReadOnly) {
            this.analyzer = analyzer;
            this.executor = executor;
            this.transportKillJobsNodeAction = transportKillJobsNodeAction;
            this.isReadOnly = isReadOnly;
        }

        Analyzer getAnalyzer() {
            return analyzer;
        }

        Executor getExecutor() {
            return executor;
        }

        TransportKillJobsNodeAction getTransportKillJobsNodeAction() {
            return transportKillJobsNodeAction;
        }

        boolean isReadOnly() {
            return isReadOnly;
        }
    }
}
