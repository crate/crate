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


import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.MatchPredicate;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.operator.AndOperator;
import io.crate.planner.consumer.QualifiedNameCollector;
import io.crate.sql.tree.QualifiedName;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class QuerySplitter {

    private static final SplitVisitor SPLIT_VISITOR = new SplitVisitor();

    /**
     * <p>
     * Splits a (function) symbol on <code>AND</code> based on relation occurrences of {@link io.crate.expression.symbol.Field}
     * into multiple symbols.
     * </p>
     * <p>
     * <h3>Examples:</h3>
     * <p>
     * Splittable down to single relation:
     * <pre>
     *     t1.x = 30 and ty2.y = 20
     *
     *     Output:
     *
     *     set(t1) -> t1.x = 30
     *     set(t2) -> t2.y = 20
     * </pre>
     * <p>
     * <p>
     * Pairs of two relations:
     * <pre>
     *     t1.x = t2.x and t2.x = t3.x
     *
     *     Output:
     *
     *     set(t1, t2) -> t1.x = t2.x
     *     set(t2, t3) -> t2.x = t3.x
     * </pre>
     * <p>
     * <p>
     * Mixed:
     * <pre>
     *     t1.x = 30 and t2.x = t3.x
     *
     *     Output:
     *
     *     set(t1)      -> t1.x = 30
     *     set(t2, t3)  -> t2.x = t3.x
     * </pre>
     */
    public static Map<Set<QualifiedName>, Symbol> split(Symbol symbol) {
        Map<Set<QualifiedName>, Symbol> splits = new LinkedHashMap<>();
        SPLIT_VISITOR.process(symbol, splits);
        return splits;
    }

    private static class SplitVisitor extends SymbolVisitor<Map<Set<QualifiedName>, Symbol>, Void> {

        @Override
        public Void visitFunction(Function function, Map<Set<QualifiedName>, Symbol> splits) {
            if (!function.info().equals(AndOperator.INFO)) {
                HashSet<QualifiedName> qualifiedNames = new LinkedHashSet<>();
                QualifiedNameCollector.INSTANCE.process(function, qualifiedNames);
                Symbol prevQuery = splits.put(qualifiedNames, function);
                if (prevQuery != null) {
                    splits.put(qualifiedNames, AndOperator.of(prevQuery, function));
                }
                return null;
            }

            for (Symbol arg : function.arguments()) {
                process(arg, splits);
            }
            return null;
        }

        @Override
        public Void visitField(Field field, Map<Set<QualifiedName>, Symbol> context) {
            context.put(Collections.singleton(field.relation().getQualifiedName()), field);
            return null;
        }

        @Override
        public Void visitMatchPredicate(MatchPredicate matchPredicate, Map<Set<QualifiedName>, Symbol> context) {
            LinkedHashSet<QualifiedName> relationNames = new LinkedHashSet<>();
            for (Field field : matchPredicate.identBoostMap().keySet()) {
                relationNames.add(field.relation().getQualifiedName());
            }
            context.put(relationNames, matchPredicate);
            return null;
        }
    }
}
