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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.Analyzer;
import io.crate.planner.DependencyCarrier;

abstract class AbstractPortal implements Portal {

    protected final String name;
    final PortalContext portalContext;
    final SessionContext sessionContext;
    boolean synced = false;

    AbstractPortal(String name, Analyzer analyzer, DependencyCarrier executor, boolean isReadOnly, SessionContext sessionContext) {
        this.name = name;
        this.sessionContext = sessionContext;
        portalContext = new PortalContext(analyzer, executor, isReadOnly);
    }

    AbstractPortal(String name, SessionContext sessionContext, PortalContext portalContext) {
        this.name = name;
        this.portalContext = portalContext;
        this.sessionContext = sessionContext;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean synced() {
        return synced;
    }

    @Override
    public String toString() {
        return "name: " + name + ", type: " + getClass().getSimpleName();
    }

    static class PortalContext {

        private final Analyzer analyzer;
        private final DependencyCarrier executor;
        private final boolean isReadOnly;

        private PortalContext(Analyzer analyzer, DependencyCarrier executor, boolean isReadOnly) {
            this.analyzer = analyzer;
            this.executor = executor;
            this.isReadOnly = isReadOnly;
        }

        Analyzer getAnalyzer() {
            return analyzer;
        }

        DependencyCarrier getExecutor() {
            return executor;
        }

        boolean isReadOnly() {
            return isReadOnly;
        }
    }
}
