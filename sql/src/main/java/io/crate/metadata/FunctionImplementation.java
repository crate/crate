/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;

import javax.annotation.Nullable;

/**
 * Base interface for function implementations.
 */
public interface FunctionImplementation {

    /**
     * Provides meta information about this function implementation.
     */
    FunctionInfo info();

    /**
     * Normalize a symbol into a simplified form.
     * This may return the symbol as is if it cannot be normalized.
     *
     * @param transactionContext context which is shared across normalizeSymbol calls during a statement-lifecycle.
     *                This will only be present if normalizeSymbol is called on the handler node.
     *                normalizeSymbol calls during execution won't receive a StmtCtx
     */
    default Symbol normalizeSymbol(Function function, @Nullable TransactionContext transactionContext) {
        return function;
    }
}
