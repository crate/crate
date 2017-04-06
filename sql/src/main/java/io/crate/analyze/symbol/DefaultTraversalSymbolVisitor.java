/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze.symbol;

public abstract class DefaultTraversalSymbolVisitor<C, R> extends SymbolVisitor<C, R> {

    /*
     * These methods don't call into super to enable subclasses to use overwrite `visitSymbol` to visit all leaves.
     * If we called super.visitXY here,
     * it would cause visitSymbol to be called for Function, FetchReference and MatchPredicate as well.
     */

    @Override
    public R visitFunction(Function symbol, C context) {
        for (Symbol arg : symbol.arguments()) {
            process(arg, context);
        }
        return null;
    }

    @Override
    public R visitFetchReference(FetchReference fetchReference, C context) {
        process(fetchReference.fetchId(), context);
        process(fetchReference.ref(), context);
        return null;
    }

    @Override
    public R visitMatchPredicate(MatchPredicate matchPredicate, C context) {
        for (Field field : matchPredicate.identBoostMap().keySet()) {
            process(field, context);
        }
        return null;
    }
}
