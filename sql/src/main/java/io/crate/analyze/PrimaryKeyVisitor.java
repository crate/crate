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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.crate.metadata.table.TableInfo;
import io.crate.operator.operator.EqOperator;
import io.crate.operator.operator.InOperator;
import io.crate.operator.operator.OrOperator;
import io.crate.planner.symbol.*;

import javax.annotation.Nullable;
import java.util.ArrayList;

public class PrimaryKeyVisitor extends SymbolVisitor<PrimaryKeyVisitor.Context, Void> {

    private static final ImmutableSet<String> PK_COMPARISONS = ImmutableSet.of(
            EqOperator.NAME, InOperator.NAME
    );

    public static class Context {

        private final TableInfo table;
        public Literal[] keyLiterals;
        public boolean noMatch = false;
        public int foundKeys = 0;
        public Literal clusteredByLiteral;
        Long version;


        public Context(TableInfo tableInfo) {
            this.table = tableInfo;
            this.keyLiterals = new Literal[tableInfo.primaryKey().size()];
        }

        public boolean noMatch() {
            return noMatch;
        }

        public ArrayList<Literal> keyLiterals() {
            if (foundKeys == table.primaryKey().size()) {
                return Lists.newArrayList(keyLiterals);
            }
            return null;
        }

        public Literal clusteredByLiteral() {
            return clusteredByLiteral;
        }

        @Nullable
        public Long version() {
            return version;
        }
    }

    @Nullable
    public Context process(TableInfo table, Function whereClause) {
        if (table.primaryKey().size() > 0 || table.clusteredBy() != null) {
            Context context = new Context(table);
            visitFunction(whereClause, context);
            return context;
        }
        return null;
    }


    @Override
    public Void visitFunction(Function symbol, Context context) {
        // ignore or
        if (symbol.info().ident().equals(OrOperator.INFO.ident())) {
            return null;
        }
        if (symbol.arguments().size() == 2 &&
                symbol.arguments().get(0).symbolType() == SymbolType.REFERENCE &&
                symbol.arguments().get(1).symbolType().isLiteral() &&
                PK_COMPARISONS.contains(symbol.info().ident().name())) {

            Literal right = (Literal)symbol.arguments().get(1);
            Reference ref = (Reference) symbol.arguments().get(0);

            if (ref.info().ident().tableIdent().equals(context.table.ident())) {
                if (ref.info().ident().columnIdent().name().equals("_version")) {
                    switch (right.symbolType()) {
                        case LONG_LITERAL:
                            context.version = ((LongLiteral)right).value();
                            break;
                        case INTEGER_LITERAL:
                            context.version = ((IntegerLiteral)right).value().longValue();
                            break;
                        default:
                            throw new IllegalArgumentException(
                                "comparison operation on \"_version\" requires a long");
                    }
                }

                if (symbol.info().ident().name().equals(EqOperator.NAME) &&
                        ref.info().ident().columnIdent().name().equals(context.table.clusteredBy())) {
                    if (context.clusteredByLiteral == null) {
                        context.clusteredByLiteral = right;
                    } else {
                        if (!context.clusteredByLiteral.equals(right)) {
                            // we cannot use the routing, since we have two values
                            context.clusteredByLiteral = null;
                        }
                    }
                }
                int idx = context.table.primaryKey().indexOf(ref.info().ident().columnIdent().name());
                if (idx >= 0) {
                    if (context.keyLiterals[idx] == null) {
                        context.foundKeys++;
                        context.keyLiterals[idx] = right;
                    } else if (!context.keyLiterals[idx].equals(right)) {
                        if (context.keyLiterals[idx] instanceof SetLiteral) {
                            if (right instanceof SetLiteral) {
                                SetLiteral intersection = ((SetLiteral) context.keyLiterals[idx]).intersection((SetLiteral) right);
                                if (intersection.size() > 0) {
                                    context.keyLiterals[idx] = intersection;
                                } else {
                                    context.noMatch = true;
                                }
                            }
                        } else {
                            context.noMatch = true;
                        }
                    }
                }
            }
            return null;
        }

        for (Symbol argument : symbol.arguments()) {
            process(argument, context);
        }
        return null;
    }
}
