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

package io.crate.operation;

import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.planner.symbol.*;

import java.util.*;

public class AssignmentSymbolVisitor extends SymbolVisitor<AssignmentSymbolVisitor.Context, Symbol> {

    public static class Context {

        protected Set<CollectExpression<?>> collectExpressions = new LinkedHashSet<>(); // to keep insertion order
        protected Map<InputColumn, InputCollectExpression> allocatedInputCollectionExpressions = new HashMap<>();

        public Set<CollectExpression<?>> collectExpressions() {
            return collectExpressions;
        }

        public InputColumn allocateCollectExpression(InputColumn inputColumn) {
            InputCollectExpression collectExpression = allocatedInputCollectionExpressions.get(inputColumn);
            if (collectExpression == null) {
                collectExpression = new InputCollectExpression(inputColumn.index());
                allocatedInputCollectionExpressions.put(inputColumn, collectExpression);
            }
            collectExpressions.add(collectExpression);
            return inputColumn;
        }

        public Input<?> collectExpressionFor(InputColumn inputColumn) {
            return allocatedInputCollectionExpressions.get(inputColumn);
        }
    }

    public Context process(Collection<Symbol> symbols) {
        Context context = new Context();
        for (Symbol symbol : symbols) {
            process(symbol, context);
        }
        return context;
    }

    @Override
    public Symbol visitReference(Reference symbol, AssignmentSymbolVisitor.Context context) {
        return symbol;
    }

    @Override
    public Symbol visitDynamicReference(DynamicReference symbol, AssignmentSymbolVisitor.Context context) {
        return visitReference(symbol, context);
    }

    @Override
    public Symbol visitFunction(Function function, AssignmentSymbolVisitor.Context context) {
        for (Symbol argument : function.arguments()) {
            process(argument, context);
        }
        return function;
    }

    @Override
    public Symbol visitLiteral(Literal symbol, AssignmentSymbolVisitor.Context context) {
        return symbol;
    }

    @Override
    public Symbol visitInputColumn(InputColumn inputColumn, AssignmentSymbolVisitor.Context context) {
        return context.allocateCollectExpression(inputColumn);
    }
}
