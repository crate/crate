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

package io.crate.planner.optimizer.rule;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.ScopedColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocTableInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.SysColumns;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.Eval;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.GroupHashAggregate;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.Order;
import io.crate.planner.operators.Rename;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

/**
 * Removes a redundant DISTINCT operation when its grouping keys
 * contain every column of the source table's primary key.
 */
public final class RemoveRedundantDistinct implements Rule<GroupHashAggregate> {

    private final Pattern<GroupHashAggregate> pattern;

    public RemoveRedundantDistinct() {
        this.pattern = typeOf(GroupHashAggregate.class)
            .with(groupBy -> groupBy.aggregates().isEmpty());
    }

    @Override
    public Pattern<GroupHashAggregate> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(
        GroupHashAggregate groupBy,
        Captures captures,
        Rule.Context context
    ) {
        if (!isUniqueByPrimaryKey(groupBy.source(), groupBy.groupKeys(), context.resolvePlan())) {
            return null;
        }

        // The grouping cannot eliminate any rows because the keys already
        // contain the complete primary key. Preserve the requested outputs
        // without performing the redundant grouping operation.
        return Eval.create(groupBy.source(), groupBy.outputs());
    }

    public static boolean isUniqueByPrimaryKey(
        LogicalPlan plan,
        List<Symbol> groupKeys,
        UnaryOperator<LogicalPlan> resolvePlan
    ) {
        if (plan == null || groupKeys == null || groupKeys.isEmpty()) {
            return false;
        }

        LogicalPlan current = resolvePlan.apply(plan);
        List<Symbol> currentKeys = groupKeys;

        while (current != null) {
            if (current instanceof Filter || current instanceof Limit || current instanceof Order) {
                // These operators do not alter the column identities or duplicate rows in a way
                // that breaks uniqueness, so we just pass through to their source.
                if (current.sources().size() != 1) {
                    return false;
                }
                current = resolvePlan.apply(current.sources().getFirst());
            } else if (current instanceof Rename rename) {
                // A Rename operator alters the symbol names. We map our current group keys
                // back to the original symbols produced by the Rename's source.
                if (rename.sources().size() != 1) {
                    return false;
                }
                LogicalPlan source = resolvePlan.apply(rename.source());
                List<Symbol> renameOutputs = rename.outputs();
                List<Symbol> sourceOutputs = source.outputs();
                if (renameOutputs.size() != sourceOutputs.size()) {
                    return false;
                }

                List<Symbol> mappedKeys = new ArrayList<>(currentKeys.size());
                for (Symbol key : currentKeys) {
                    // Match the unwrapped expression so aliases do not prevent identifying
                    // a grouping key that originates from the source plan.
                    Symbol unwrappedKey = unwrapAlias(key);
                    int idx = -1;
                    for (int i = 0; i < renameOutputs.size(); i++) {
                        if (unwrapAlias(renameOutputs.get(i)).equals(unwrappedKey)) {
                            idx = i;
                            break;
                        }
                    }
                    if (idx < 0) {
                        return false;
                    }
                    mappedKeys.add(sourceOutputs.get(idx));
                }
                currentKeys = mappedKeys;
                current = source;
            } else if (current instanceof Eval eval) {
                if (eval.sources().size() != 1) {
                    return false;
                }
                LogicalPlan source = resolvePlan.apply(eval.source());
                List<Symbol> evalOutputs = eval.outputs();
                List<Symbol> sourceOutputs = source.outputs();

                // Only propagate keys that are preserved by Eval as direct source
                // outputs. Computed expressions are not considered when proving
                // primary-key uniqueness.
                List<Symbol> mappedKeys = new ArrayList<>(currentKeys.size());
                for (Symbol key : currentKeys) {
                    Symbol unwrappedKey = unwrapAlias(key);
                    boolean foundInEval = false;
                    for (Symbol evalOut : evalOutputs) {
                        if (unwrapAlias(evalOut).equals(unwrappedKey)) {
                            foundInEval = true;
                            break;
                        }
                    }
                    if (!foundInEval) {
                        return false;
                    }

                    // Ensure the key can also be traced to an actual source output.
                    // Otherwise, do not assume that the expression preserves uniqueness.
                    Symbol matchingSourceOutput = null;
                    for (Symbol srcOut : sourceOutputs) {
                        if (unwrapAlias(srcOut).equals(unwrappedKey)) {
                            matchingSourceOutput = srcOut;
                            break;
                        }
                    }
                    if (matchingSourceOutput == null) {
                        return false;
                    }
                    mappedKeys.add(matchingSourceOutput);
                }
                currentKeys = mappedKeys;
                current = source;
            } else if (current instanceof Collect collect) {
                if (!(collect.relation().tableInfo() instanceof DocTableInfo tableInfo)) {
                    return false;
                }

                // If the PK is auto-generated (like _id), user columns can't guarantee uniqueness
                if (tableInfo.hasAutoGeneratedPrimaryKey()) {
                    return false;
                }

                var primaryKey = tableInfo.primaryKey();
                if (primaryKey.isEmpty() || primaryKey.contains(SysColumns.ID.COLUMN)) {
                    return false;
                }
                RelationName tableIdent = tableInfo.ident();
                Set<ColumnIdent> groupedColumns = new HashSet<>();

                for (Symbol key : currentKeys) {
                    Symbol unwrapped = unwrapAlias(key);
                    if (unwrapped instanceof ScopedColumn scopedColumn) {
                        if (scopedColumn.relation().equals(tableIdent)) {
                            groupedColumns.add(scopedColumn.column());
                        }
                    }
                }

                // The rows are uniquely identifiable if every column of the primary key
                // is present in our grouping columns.
                return groupedColumns.containsAll(primaryKey);
            } else {
                // Joins, unions, aggregates and other unsupported operators may change
                // row multiplicity or key identity. Retain DISTINCT unless uniqueness
                // can be proven
                return false;
            }
        }
        return false;
    }

    private static Symbol unwrapAlias(Symbol symbol) {
        while (symbol instanceof AliasSymbol alias) {
            symbol = alias.symbol();
        }
        return symbol;
    }
}
