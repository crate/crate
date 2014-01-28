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

package io.crate.analyze;

import io.crate.planner.symbol.BooleanLiteral;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;

public class NormalizationHelper {

    /**
     * return <code>true</code> if this function evaluates to <code>false</code> or <code>null</code> using the given <code>normalizer</code>.
     *
     * @param function
     * @param normalizer
     * @return false if whereClause evaluates to <code>false</code> or {@link io.crate.planner.symbol.Null}
     *         so no query has to be executed, <code>true</code> otherwise
     */
    public static boolean evaluatesToFalse(Function function, EvaluatingNormalizer normalizer) {
        Symbol normalizedWhereClause = normalizer.process(function, null);
        return (normalizedWhereClause.symbolType() == SymbolType.NULL_LITERAL ||
                (normalizedWhereClause.symbolType() == SymbolType.BOOLEAN_LITERAL &&
                        !((BooleanLiteral) normalizedWhereClause).value()));
    }
}
