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

import io.crate.metadata.settings.SessionSettings;
import io.crate.planner.operators.LogicalPlanIdAllocator;

import java.time.Instant;

public interface TransactionContext {

    static TransactionContext of(SessionSettings sessionSettings) {
        return new StaticTransactionContext(sessionSettings, new LogicalPlanIdAllocator());
    }

    Instant currentInstant();

    SessionSettings sessionSettings();

    LogicalPlanIdAllocator idAllocator();

    class StaticTransactionContext implements TransactionContext {

        private final SessionSettings sessionSettings;
        private Instant currentInstant;
        private final LogicalPlanIdAllocator idAllocator;


        StaticTransactionContext(SessionSettings sessionSettings, LogicalPlanIdAllocator idAllocator) {
            this.sessionSettings = sessionSettings;
            this.idAllocator = idAllocator;
        }

        @Override
        public Instant currentInstant() {
            if (currentInstant == null) {
                currentInstant = SystemClock.currentInstant();
            }
            return currentInstant;
        }

        @Override
        public SessionSettings sessionSettings() {
            return sessionSettings;
        }

        @Override
        public LogicalPlanIdAllocator idAllocator() {
            return idAllocator;
        }

    }
}
