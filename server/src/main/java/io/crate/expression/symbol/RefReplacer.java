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

package io.crate.expression.symbol;

import static java.util.Objects.requireNonNull;

import java.util.function.Function;

import io.crate.metadata.ScopedRef;

public final class RefReplacer extends FunctionCopyVisitor<Function<? super ScopedRef, ? extends Symbol>> {

    private static final RefReplacer REPLACER = new RefReplacer();

    private RefReplacer() {
        super();
    }

    /**
     * Applies {@code mapper} on all {@link ScopedRef} instances within {@code tree}
     */
    public static Symbol replaceRefs(Symbol tree, Function<? super ScopedRef, ? extends Symbol> mapper) {
        return tree.accept(REPLACER, mapper);
    }

    @Override
    public Symbol visitReference(ScopedRef ref, Function<? super ScopedRef, ? extends Symbol> mapper) {
        return requireNonNull(mapper.apply(ref), "mapper function used in RefReplacer must not return null values");
    }
}
