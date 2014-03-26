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
import com.google.common.collect.Sets;
import io.crate.DataType;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.InOperator;
import io.crate.operation.operator.OrOperator;
import io.crate.planner.symbol.*;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Visitor which traverses functions to find primary key columns, _version and clustered by columns
 *
 * for each "or" node a new bucket is created which can
 *  contain its own primary key columns, version and clustered by info
 *
 * e.g.
 *
 *  where (pkcol1 = 1 and pkcol2 = 2 and _version = 1) or (pkcol = 10 and pkcol2 = 20 and _version = 2)
 *
 *  would create two buckets with keyParts  and version set.
 *
 *  for backward compatibility the information in the context is exposed in a way that version and clusteredBy
 *  is only exposed if primary key has only 1 key part.
 *
 *  if any column that is neither primary key, nor _version or clustered by column is encountered
 *  the whole context is invalidated.
 */
public class PrimaryKeyVisitor extends SymbolVisitor<PrimaryKeyVisitor.Context, Void> {

    private static final ImmutableSet<String> logicalBinaryExpressions = ImmutableSet.of(
            AndOperator.NAME, OrOperator.NAME
    );
    private static final ImmutableSet<String> PK_COMPARISONS = ImmutableSet.of(
            EqOperator.NAME, InOperator.NAME
    );

    public static class Context {

        private final TableInfo table;
        private final ArrayList<KeyBucket> buckets;

        private KeyBucket currentBucket;
        public boolean invalid = false;
        public boolean versionInvalid = false;

        private List keyLiterals;
        private Long version;
        private Literal clusteredBy;
        public boolean noMatch = false;

        public Context(TableInfo tableInfo) {
            this.table = tableInfo;
            this.buckets = new ArrayList<>();
            newBucket();
        }

        /**
         * For compound primary keys we cannot use (array)literals here,
         * because primary key literals can differ in type.
         * We must return a nested list of primitive literals in such cases.
         *
         * @return
         */
        @Nullable
        public List keyLiterals() {
            return keyLiterals;
        }

        @Nullable
        public Long version() {
            return version;
        }

        public Literal clusteredByLiteral() {
            return clusteredBy;
        }

        public String clusteredBy() {
            if (clusteredBy != null) {
                return clusteredBy.valueAsString();
            }
            return null;
        }

        void newBucket() {
            currentBucket = new KeyBucket(table.primaryKey().size());
            buckets.add(currentBucket);
        }

        void finish() {
            if (!versionInvalid && buckets.size() == 1) {
                version = currentBucket.version;
            }
            if (!invalid) {
                if (buckets.size() == 1) {
                    clusteredBy = currentBucket.clusteredBy;

                    if (currentBucket.partsFound == table.primaryKey().size()) {
                        keyLiterals = Lists.newArrayList(currentBucket.keyParts);
                    }
                } else if (table.primaryKey().size() == 1) {
                    Set<Literal> keys = new HashSet<>();
                    DataType keyType = DataType.NULL;
                    for (KeyBucket bucket : buckets) {
                        Literal keyPart = bucket.keyParts[0];
                        if (keyPart != null) {
                            keyType = keyPart.valueType();
                            if (keyPart instanceof SetLiteral) {
                                keys.addAll(((SetLiteral) keyPart).literals());
                            } else {
                                keys.add(keyPart);
                            }
                        }
                    }
                    keyLiterals = Arrays.<Literal>asList(SetLiteral.fromLiterals(keyType, keys));
                } else {
                    // support compound primary keys
                    for (int b=0; b<buckets.size();b++) {
                        KeyBucket bucket = buckets.get(b);
                        if (bucket.partsFound == table.primaryKey().size()) {
                            if (keyLiterals == null) {
                                keyLiterals = new ArrayList<List>();
                            }
                            List keys = new ArrayList<Object>();
                            keyLiterals.add(keys);
                            for (int i=0; i<bucket.keyParts.length; i++) {
                                Literal keyPart = bucket.keyParts[i];
                                if (keyPart != null) {
                                    if (keyPart.symbolType() != SymbolType.SET_LITERAL) {
                                        keys.add(keyPart);
                                    }
                                }
                            }
                        }
                    }

                    if (keyLiterals != null && keyLiterals.size() != buckets.size()) {
                        // not all parts of all buckets found
                        keyLiterals = null;
                    }

                }
            }
        }
    }

    private static class KeyBucket {
        private final Literal[] keyParts;
        Long version;
        public Literal clusteredBy;
        public int partsFound = 0;

        KeyBucket(int numKeys) {
            keyParts = new Literal[numKeys];
        }
    }

    @Nullable
    public Context process(TableInfo table, Symbol whereClause) {
        if (table.primaryKey().size() > 0 || table.clusteredBy() != null) {
            Context context = new Context(table);
            process(whereClause, context);
            context.finish();
            return context;
        }
        return null;
    }

    @Override
    public Void visitReference(Reference reference, Context context) {
        String columnName = reference.info().ident().columnIdent().name();
        if (!reference.info().ident().tableIdent().equals(context.table.ident())) {
            return invalidate(context);
        }

        // where booleanCol; can be handled like: where booleanCol = true;
        if (reference.valueType() == DataType.BOOLEAN) {
            if (columnName.equals(context.table.clusteredBy())) {
                setClusterBy(context, BooleanLiteral.TRUE);
            }
            int idx = context.table.primaryKey().indexOf(columnName);
            if (idx >= 0) {
                setPrimaryKey(context, BooleanLiteral.TRUE, idx);
            }
        }

        return null;
    }

    @Override
    public Void visitFunction(Function function, Context context) {
        String functionName = function.info().ident().name();
        if (logicalBinaryExpressions.contains(functionName)) {
            return continueTraversal(function, context);
        }
        if (!PK_COMPARISONS.contains(functionName)) {
            return invalidate(context);
        }

        assert function.arguments().size() == 2; // pk comparison functions can't have more than 2
        Symbol left = function.arguments().get(0);
        Symbol right = function.arguments().get(1);

        if (left.symbolType() != SymbolType.REFERENCE || !right.symbolType().isLiteral()) {
            return invalidate(context);
        }

        Reference reference = (Reference)left;
        String columnName = reference.info().ident().columnIdent().name();
        if (!reference.info().ident().tableIdent().equals(context.table.ident())) {
            return invalidate(context);
        }

        boolean clusteredBySet = false;
        if (functionName.equals(EqOperator.NAME)) {
            if (columnName.equals(context.table.clusteredBy())) {
                setClusterBy(context, (Literal)right);
                clusteredBySet = true;
            }
            if (columnName.equals("_version")) {
                setVersion(context, right);
                return null;
            }
        }

        int idx = context.table.primaryKey().indexOf(columnName);
        if (idx < 0 && !clusteredBySet) {
            // do not return here, as we might be searching for version
            invalidate(context);
        } else if (idx >= 0) {
            setPrimaryKey(context, (Literal)right, idx);
        }
        return null;
    }

    private void setPrimaryKey(Context context, Literal right, int idx) {
        if (context.currentBucket.keyParts[idx] == null) {
            context.currentBucket.keyParts[idx] = right;
            context.currentBucket.partsFound++;
        } else if (!context.currentBucket.keyParts[idx].equals(right)) {
            if (context.currentBucket.keyParts[idx] instanceof SetLiteral) {
                SetLiteral intersection;
                if (right instanceof SetLiteral) {
                    intersection = ((SetLiteral) context.currentBucket.keyParts[idx]).intersection((SetLiteral) right);
                } else {
                    intersection = ((SetLiteral) context.currentBucket.keyParts[idx])
                            .intersection(SetLiteral.fromLiterals(right.valueType(), Sets.newHashSet(right)));
                }

                if (intersection.size() > 0) {
                    context.currentBucket.keyParts[idx] = intersection;
                    return;
                }
            }

            /**
             * if we get to this point we've had something like
             *      where pkCol = 1 and pkCol = 2
             * or
             *      where pkCol in (1, 2) and pkCol = 3
             *
             * which can never match so the query doesn't need to be executed.
             */
            context.noMatch = true;
        }
    }

    private void setClusterBy(Context context, Literal right) {
        if (context.currentBucket.clusteredBy == null) {
            context.currentBucket.clusteredBy = right;
        } else if (!context.currentBucket.clusteredBy.equals(right)) {
            invalidate(context);
        }
    }

    private Void invalidate(Context context) {
        context.invalid = true;
        return null;
    }

    private Void invalidateVersion(Context context) {
        context.versionInvalid = true;
        return null;
    }

    private void setVersion(Context context, Symbol right) {
        Long version;
        switch (right.symbolType()) {
            case LONG_LITERAL:
                version = ((LongLiteral)right).value();
                break;
            case INTEGER_LITERAL:
                version = ((IntegerLiteral)right).value().longValue();
                break;
            default:
                throw new IllegalArgumentException(
                        "comparison operation on \"_version\" requires a long");
        }

        if (context.currentBucket.version != null && !context.currentBucket.version.equals(version)) {
            invalidateVersion(context);
        } else {
            context.currentBucket.version = version;
        }
    }

    private Void continueTraversal(Function symbol, Context context) {
        String functionName = symbol.info().ident().name();

        boolean newBucket = true;
        for (Symbol argument : symbol.arguments()) {
            process(argument, context);
            if (newBucket && functionName.equals(OrOperator.NAME)) {
                newBucket = false;
                context.newBucket();
            }
        }
        return null;
    }
}
