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

package io.crate.analyze.relations;


import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.operation.operator.AndOperator;
import io.crate.planner.consumer.ManyTableConsumer;
import io.crate.sql.tree.QualifiedName;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class QuerySplitter {

    private static final SplitVisitor SPLIT_VISITOR = new SplitVisitor();

    /**
     * <p>
     * splits a (function) symbol into multiple symbols where each symbol has 2 or more relations.
     * </p>
     *
     * E.g.
     * <pre>
     *     t1.x = t2.x and t2.x = t3.x
     * </pre>
     * Will be split into:
     *
     * <pre>
     *     t1.x = t2.x  ->  t1, t2
     *     t2.x = t3.x  ->  t2, t3
     * </pre>
     */
    public static Map<Symbol, Collection<QualifiedName>> split(Symbol symbol) {
        Context context = new Context();
        SPLIT_VISITOR.process(symbol, context);
        return context.splits;
    }

    private static class Context {

        public Map<Symbol, Collection<QualifiedName>> splits = new HashMap<>();
    }

    private static class SplitVisitor extends SymbolVisitor<Context, Void> {

        @Override
        public Void visitFunction(Function function, Context context) {
            if (!function.info().equals(AndOperator.INFO)) {
                HashSet<QualifiedName> qualifiedNames = new HashSet<>();
                ManyTableConsumer.QualifiedNameCounter.INSTANCE.process(function, qualifiedNames);
                context.splits.put(function, qualifiedNames);
                return null;
            }

            for (Symbol arg : function.arguments()) {
                process(arg, context);
            }
            return null;
        }
    }
}
