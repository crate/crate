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

import io.crate.action.sql.SessionContext;
import io.crate.metadata.settings.SessionSettings;

import java.time.Instant;
import java.util.Objects;

/**
 * TransactionContext is a context that is used to keep state which is valid during a transaction.
 *
 * In Crate a transaction is always bound to the lifecycle of a single query/statement execution as there is no
 * transaction support.
 */
public final class CoordinatorTxnCtx implements TransactionContext {

    private final SessionContext sessionContext;
    private Instant currentInstant;

    public static CoordinatorTxnCtx systemTransactionContext() {
        return new CoordinatorTxnCtx(SessionContext.systemSessionContext());
    }

    public CoordinatorTxnCtx(SessionContext sessionContext) {
        this.sessionContext = Objects.requireNonNull(sessionContext);
    }

    /**
     * @return current Instant. Subsequent calls will always return the same value. (Not thread-safe)
     */
    @Override
    public Instant currentInstant() {
        // no synchronization because StmtCtx is mostly used during single-threaded analysis phase
        if (currentInstant == null) {
            currentInstant = SystemClock.currentInstant();
        }
        return currentInstant;
    }

    @Override
    public SessionSettings sessionSettings() {
        return new SessionSettings(sessionContext.sessionUser().name(),
                                   sessionContext.searchPath(),
                                   sessionContext.isHashJoinEnabled(),
                                   sessionContext.excludedOptimizerRules(),
                                   sessionContext.errorOnUnknownObjectKey());
    }

    public SessionContext sessionContext() {
        return sessionContext;
    }
}
