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

package io.crate.analyze;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.expression.BaseImplementationSymbolVisitor;
import io.crate.expression.InputFactory;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.types.DataType;

/**
 * Used to evaluate a symbol tree to a value.
 *
 * - This should be preferred over {@link EvaluatingNormalizer} if it's required to fully evaluate the tree
 * - This should be preferred over {@link InputFactory} if it's not used repeatedly.
 *   (Unless column evaluation is necessary)
 *
 * This does not handle Columns/InputColumns, only Functions, Literals, ParameterSymbols and SubQuery values
 */
public final class SymbolEvaluator extends BaseImplementationSymbolVisitor<Row> {


    private final SubQueryResults subQueryResults;

    private SymbolEvaluator(TransactionContext txnCtx, Functions functions, SubQueryResults subQueryResults) {
        super(txnCtx, functions);
        this.subQueryResults = subQueryResults;
    }

    public static Object evaluateWithoutParams(TransactionContext txn, Functions functions, Symbol symbol) {
        return evaluate(txn, functions, symbol, Row.EMPTY, SubQueryResults.EMPTY);
    }

    public static Object evaluate(TransactionContext txnCtx,
                                  Functions functions,
                                  Symbol symbol,
                                  Row params,
                                  SubQueryResults subQueryValues) {
        SymbolEvaluator symbolEval = new SymbolEvaluator(txnCtx, functions, subQueryValues);
        return symbol.accept(symbolEval, params).value();
    }

    @Override
    public Input<?> visitParameterSymbol(ParameterSymbol parameterSymbol, Row params) {
        return () -> params.get(parameterSymbol.index());
    }

    @Override
    public Input<?> visitSelectSymbol(SelectSymbol selectSymbol, Row context) {
        DataType type = selectSymbol.valueType();
        return Literal.of(type, type.value(subQueryResults.getSafe(selectSymbol)));
    }
}
