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

package io.crate.planner.fetch;

import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.DynamicReference;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;

import java.util.Collection;

public final class FetchFeasibility extends SymbolVisitor<Collection<? super Symbol>, Boolean> {

    private static final FetchFeasibility INSTANCE = new FetchFeasibility();

    private FetchFeasibility() {
    }

    static boolean isFetchFeasible(Iterable<? extends Symbol> outputs,
                                   Collection<? super Symbol> querySymbols) {
        for (Symbol output : outputs) {
            boolean fetchIsPossible = INSTANCE.process(output, querySymbols);
            if (fetchIsPossible) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visitReference(Reference ref, Collection<? super Symbol> querySymbols) {
        if (querySymbols.contains(ref) || ref.column().equals(DocSysColumns.SCORE)) {
            return false;
        }
        return true;
    }

    @Override
    public Boolean visitField(Field field, Collection<? super Symbol> querySymbols) {
        if (querySymbols.contains(field) || field.path().equals(DocSysColumns.SCORE)) {
            return false;
        }
        return true;
    }

    @Override
    public Boolean visitDynamicReference(DynamicReference symbol, Collection<? super Symbol> querySymbols) {
        return visitReference(symbol, querySymbols);
    }

    @Override
    protected Boolean visitSymbol(Symbol symbol, Collection<? super Symbol> querySymbols) {
        return false;
    }

    @Override
    public Boolean visitAggregation(Aggregation aggregation, Collection<? super Symbol> querySymbols) {
        return !querySymbols.contains(aggregation)
               && isFetchFeasible(aggregation.inputs(), querySymbols);
    }

    @Override
    public Boolean visitFunction(Function function, Collection<? super Symbol> querySymbols) {
        return !querySymbols.contains(function)
               && isFetchFeasible(function.arguments(), querySymbols);
    }
}
