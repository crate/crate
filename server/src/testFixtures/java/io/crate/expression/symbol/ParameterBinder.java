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

package io.crate.expression.symbol;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class ParameterBinder extends FunctionCopyVisitor<Function<? super ParameterSymbol, ? extends Symbol>> {

    private static final ParameterBinder BINDER = new ParameterBinder();

    private ParameterBinder() {
        super();
    }

    /**
     * Applies {@code mapper} on all {@link ParameterSymbol} instances within {@code tree}
     */
    public static Symbol bindParameters(Symbol tree, Function<? super ParameterSymbol, ? extends Symbol> mapper) {
        return tree.accept(BINDER, mapper);
    }

    @Override
    public Symbol visitParameterSymbol(ParameterSymbol parameter, Function<? super ParameterSymbol, ? extends Symbol> mapper) {
        return requireNonNull(mapper.apply(parameter), "mapper function used in ParameterBinder must not return null values");
    }
}
