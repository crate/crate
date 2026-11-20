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
import io.crate.planner.operators.ForwardingLogicalPlan;
import io.crate.planner.operators.GroupHashAggregate;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.LimitDistinct;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.Order;
import io.crate.planner.operators.Rename;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

/**
 * Removes a redundant DISTINCT operation when its grouping keys
 * contain every column of the source table's primary key.
 *
 * <p>Handles both forms a key-only DISTINCT / GROUP BY can take in the plan:
 * <ul>
 *   <li>{@link GroupHashAggregate} without aggregates is replaced by an {@link Eval} of its source.</li>
 *   <li>{@link LimitDistinct}, created by {@link RewriteGroupByKeysLimitToLimitDistinct} from
 *   {@code Limit -> GroupHashAggregate}, is replaced by a plain {@link Limit}.</li>
 * </ul>
 * Matching both keeps the result independent of the order in which the optimizer applies this
 * rule and {@link RewriteGroupByKeysLimitToLimitDistinct}.
 */
public final class RemoveRedundantDistinct implements Rule<ForwardingLogicalPlan> {

    private final Pattern<ForwardingLogicalPlan> pattern;

    public RemoveRedundantDistinct() {
        this.pattern = typeOf(ForwardingLogicalPlan.class)
            .with(plan -> plan instanceof LimitDistinct
                || (plan instanceof GroupHashAggregate groupBy && groupBy.aggregates().isEmpty()));
    }

    @Override
    public Pattern<ForwardingLogicalPlan> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(
        ForwardingLogicalPlan plan,
        Captures captures,
        Rule.Context context
    ) {
        LogicalPlan source = plan.source();
        // LimitDistinct's outputs are its distinct keys
        List<Symbol> keys = plan instanceof GroupHashAggregate groupBy ? groupBy.groupKeys() : plan.outputs();
        if (!isUniqueByPrimaryKey(source, keys, context.resolvePlan())) {
            return null;
        }

        // The grouping cannot eliminate any rows because the keys already
        // contain the complete primary key. Preserve the requested outputs
        // without performing the redundant grouping operation.
        // This also removes the de-duplicating GROUP BY, added by RewriteDistinctAggToGroupBy
        // for e.g. `COUNT(DISTINCT pk)`.
        LogicalPlan withoutDistinct = Eval.create(source, plan.outputs());
        if (plan instanceof LimitDistinct limitDistinct) {
            // De-duplication cannot drop rows, so limit/offset have the same effect on the plain source
            return new Limit(withoutDistinct, limitDistinct.limit(), limitDistinct.offset());
        }
        return withoutDistinct;
    }

    private static boolean isUniqueByPrimaryKey(
        LogicalPlan plan,
        List<Symbol> groupKeys,
        UnaryOperator<LogicalPlan> resolvePlan
    ) {
        if (plan == null || groupKeys == null || groupKeys.isEmpty()) {
            return false;
        }

        LogicalPlan current = resolvePlan.apply(plan);
        List<Symbol> currentKeys = groupKeys;

        while (true) {
            if (current instanceof Filter || current instanceof Limit || current instanceof Order) {
                // These operators do not alter the column identities or duplicate rows in a way
                // that breaks uniqueness, so we just pass through to their source.
                current = resolvePlan.apply(current.sources().getFirst());
            } else if (current instanceof Rename rename) {
                // A Rename operator alters the symbol names. We map our current group keys
                // back to the original symbols produced by the Rename's source.
                // This relies on Rename's outputs being positionally aligned with its source's outputs.
                current = resolvePlan.apply(rename.source());
                currentKeys = mapKeysToOutputs(
                    currentKeys,
                    rename.outputs(),
                    current.outputs()
                );
                if (currentKeys == null) {
                    return false;
                }
            } else if (current instanceof Eval eval) {
                current = resolvePlan.apply(eval.source());
                currentKeys = mapKeysToOutputs(
                    currentKeys,
                    current.outputs(),
                    current.outputs()
                );
                if (currentKeys == null) {
                    return false;
                }
            } else if (current instanceof Collect collect) {
                if (!(collect.relation().tableInfo() instanceof DocTableInfo tableInfo)) {
                    return false;
                }

                // If the PK is auto-generated (like _id), user columns can't guarantee uniqueness
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
    }

    private static Symbol unwrapAlias(Symbol symbol) {
        while (symbol instanceof AliasSymbol alias) {
            symbol = alias.symbol();
        }
        return symbol;
    }

    private static int findOutputIndex(List<Symbol> outputs, Symbol key) {
        // Match the unwrapped expression so aliases do not prevent identifying
        // a grouping key that originates from the source plan.
        Symbol unwrappedKey = unwrapAlias(key);
        for (int i = 0; i < outputs.size(); i++) {
            if (unwrapAlias(outputs.get(i)).equals(unwrappedKey)) {
                return i;
            }
        }
        return -1;
    }

    private static List<Symbol> mapKeysToOutputs(
        List<Symbol> keys,
        List<Symbol> matchingOutputs,
        List<Symbol> targetOutputs
    ) {
        List<Symbol> mappedKeys = new ArrayList<>(keys.size());
        for (Symbol key : keys) {
            int idx = findOutputIndex(matchingOutputs, key);
            if (idx < 0 || idx >= targetOutputs.size()) {
                return null;
            }
            mappedKeys.add(targetOutputs.get(idx));
        }
        return mappedKeys;
    }
}
