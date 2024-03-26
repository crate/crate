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

package io.crate.expression.operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import io.crate.data.Input;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.planner.operators.Filter;
import io.crate.types.DataTypes;

public class AndOperator extends Operator<Boolean> {

    public static final String NAME = "op_and";
    public static final Signature SIGNATURE = Signature.scalar(
        NAME,
        DataTypes.BOOLEAN.getTypeSignature(),
        DataTypes.BOOLEAN.getTypeSignature(),
        DataTypes.BOOLEAN.getTypeSignature()
    );

    public static void register(Functions.Builder builder) {
        builder.add(SIGNATURE, AndOperator::new);
    }

    public AndOperator(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public Symbol normalizeSymbol(Function function, TransactionContext txnCtx, NodeContext nodeCtx) {
        assert function != null : "function must not be null";
        assert function.arguments().size() == 2 : "number of args must be 2";

        Symbol left = function.arguments().get(0);
        Symbol right = function.arguments().get(1);

        if (left instanceof Input && right instanceof Input) {
            return Literal.of(evaluate(txnCtx, nodeCtx, (Input) left, (Input) right));
        }

        /**
         * true  and x  -> x
         * false and x  -> false
         * null  and x  -> false or null -> function as is
         */
        if (left instanceof Input leftInput) {
            Object value = leftInput.value();
            if (value == null) {
                return function;
            }
            if ((Boolean) value) {
                return right;
            } else {
                return Literal.of(false);
            }
        }
        if (right instanceof Input<?> rightInput) {
            Object value = rightInput.value();
            if (value == null) {
                return function;
            }
            if ((Boolean) value) {
                return left;
            } else {
                return Literal.of(false);
            }
        }
        return function;
    }

    @Override
    @SafeVarargs
    public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Boolean>... args) {
        assert args != null : "args must not be null";
        assert args.length == 2 : "number of args must be 2";
        assert args[0] != null && args[1] != null : "1st and 2nd arguments must not be null";

        // implement three valued logic.
        // don't touch anything unless you have a good reason for it! :)
        // http://en.wikipedia.org/wiki/Three-valued_logic
        Boolean left = args[0].value();
        Boolean right = args[1].value();

        if (left == null && right == null) {
            return null;
        }

        if (left == null) {
            return (!right) ? false : null;
        }

        if (right == null) {
            return (!left) ? false : null;
        }

        return left && right;
    }

    @Override
    public Query toQuery(Function function, Context context) {
        BooleanQuery.Builder query = new BooleanQuery.Builder();
        for (Symbol symbol : function.arguments()) {
            query.add(symbol.accept(context.visitor(), context), BooleanClause.Occur.MUST);
        }
        return query.build();
    }

    public static Function of(Symbol first, Symbol second) {
        assert first.valueType().equals(DataTypes.BOOLEAN) || first.valueType().equals(DataTypes.UNDEFINED) :
            "first symbol must have BOOLEAN return type to create AND function";
        assert second.valueType().equals(DataTypes.BOOLEAN) || second.valueType().equals(DataTypes.UNDEFINED) :
            "second symbol must have BOOLEAN return type to create AND function";

        return new Function(SIGNATURE, List.of(first, second), Operator.RETURN_TYPE);
    }

    public static Symbol join(Iterable<? extends Symbol> symbols) {
        return join(symbols.iterator());
    }

    public static Symbol join(Iterator<? extends Symbol> symbols) {
        return join(symbols, Literal.BOOLEAN_TRUE);
    }

    public static Symbol join(Iterable<? extends Symbol> symbols, Symbol defaultSymbol) {
        return join(symbols.iterator(), defaultSymbol);
    }

    public static Symbol join(Iterator<? extends Symbol> symbols, Symbol defaultSymbol) {
        if (!symbols.hasNext()) {
            return defaultSymbol;
        }
        Symbol first = symbols.next();
        while (symbols.hasNext()) {
            var next = symbols.next();
            if (!Filter.isMatchAll(next)) {
                first = new Function(SIGNATURE, List.of(first, next), Operator.RETURN_TYPE);
            }
        }
        return first;
    }

    /**
     * Split a symbol by AND functions.
     * <pre>
     * x = 1 AND y = 2 ->  [(x = 1), y = 2)]
     * x = 1           ->  [(x = 1)]
     * </pre>
     *
     * @return The parts of a predicate
     */
    public static List<Symbol> split(Symbol predicate) {
        ArrayList<Symbol> conjunctions = new ArrayList<>();
        predicate.accept(SplitVisitor.INSTANCE, conjunctions);
        if (conjunctions.isEmpty()) {
            conjunctions.add(predicate);
        }
        return conjunctions;
    }

    static class SplitVisitor extends SymbolVisitor<List<Symbol>, Symbol> {

        private static final SplitVisitor INSTANCE = new SplitVisitor();

        @Override
        protected Symbol visitSymbol(Symbol symbol, List<Symbol> context) {
            return symbol;
        }

        @Override
        public Symbol visitFunction(Function func, List<Symbol> conjunctions) {
            var signature = func.signature();
            assert signature != null : "Expecting functions signature not to be null";
            if (signature.equals(SIGNATURE)) {
                for (Symbol argument : func.arguments()) {
                    Symbol result = argument.accept(this, conjunctions);
                    if (result != null) {
                        conjunctions.add(result);
                    }
                }
                return null;
            } else {
                return func;
            }
        }
    }
}
