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

import org.jspecify.annotations.Nullable;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;

/**
 * Base interface for function implementations.
 */
public interface FunctionImplementation {

    /**
     * Return the declared signature for this implementation.
     * All function implementations are registered under their declared signature, thus function symbols must
     * carry the declared signature in order to look up each implementation for execution.
     */
    Signature signature();

    /**
     * Return the bound signature for this implementation.
     * This signature has all actual argument types bound, possible type variables of the declared signature are
     * replaced.
     *
     * Bound argument and return types are required by function symbols.
     */
    BoundSignature boundSignature();

    /**
     * Normalize a symbol into a simplified form.
     * This may return the symbol as is if it cannot be normalized.
     *
     * @param txnCtx context which is shared across normalizeSymbol calls during a statement-lifecycle.
     *                This will only be present if normalizeSymbol is called on the handler node.
     *                normalizeSymbol calls during execution won't receive a StmtCtx
     *
     * @param nodeCtx context which is shared across normalizeSymbol calls during a statement-lifecycle.
     *                Contains a reference to Functions.
     */
    default Symbol normalizeSymbol(Function function, @Nullable TransactionContext txnCtx, NodeContext nodeCtx) {
        return function;
    }
}
