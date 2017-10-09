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

package io.crate.planner.operators;

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.symbol.FieldReplacer;
import io.crate.analyze.symbol.RefReplacer;
import io.crate.analyze.symbol.Symbol;
import io.crate.collections.Lists2;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * LogicalPlan is a tree of "Operators"
 * This is a representation of the logical order of operators that need to be executed to produce a correct result.
 *
 * {@link #build(Planner.Context, ProjectionBuilder, int, int, OrderBy, Integer)} is used to create the
 * actual "physical" execution plan.
 *
 * A Operator is something like Limit, OrderBy, HashAggregate, Join, Union, Collect
 * <pre>
 *     select x, y, z from t1 where x = 10 order by x limit 10:
 *
 *     Limit 10
 *        |
 *     Order By x
 *         |
 *     Collect [x, y, z]
 * </pre>
 *
 * {@link #build(Planner.Context, ProjectionBuilder, int, int, OrderBy, Integer)} is called on the "root" and flows down.
 * Each time each operator may provide "hints" to the children so that they can decide to eagerly apply parts of the
 * operations
 *
 * This allows us to create execution plans as follows::
 *
 * <pre>
 *     select x, y, z from t1 where order by x limit 10;
 *
 *
 *          Merge
 *         /  limit10
 *       /         \
 *     Collect     Collect
 *     limit 10    limit 10
 *
 * </pre>
 */
public interface LogicalPlan {

    static List<Symbol> mappedSymbols(List<Symbol> sourceOutputs, Map<Symbol, Symbol> mapping) {
        if (mapping.isEmpty()) {
            return sourceOutputs;
        }
        return Lists2.copyAndReplace(sourceOutputs, getMapper(mapping));
    }

    static Function<Symbol, Symbol> getMapper(Map<Symbol, Symbol> mapping) {
        return s -> {
            Symbol mapped = mapping.get(s);
            if (mapped != null) {
                return mapped;
            }
            mapped = FieldReplacer.replaceFields(s, f -> mapping.getOrDefault(f, f));
            if (mapped != s) {
                return mapped;
            }
            mapped = RefReplacer.replaceRefs(s, r -> mapping.getOrDefault(r, r));
            return mapped;
        };
    }

    interface Builder {

        /**
         * Create a LogicalPlan node
         *
         * @param usedBeforeNextFetch The columns the "parent" is using.
         *                    This is used to create plans which utilize query-then-fetch.
         *                    For example:
         *                    <pre>
         *                       select a, b, c from t1 order by a limit 10
         *
         *                       EvalFetch (usedColumns: [a, b, c])
         *                         outputs: [a, b, c]   (b, c resolved using _fetch)
         *                         |
         *                       Limit 10 (usedColumns: [])
         *                         |
         *                       Order (usedColumns: [a]
         *                         |
         *                       Collect (usedColumns: [a] - inherited from Order)
         *                         outputs: [_fetch, a]
         *                    </pre>
         */
        LogicalPlan build(Set<Symbol> usedBeforeNextFetch);
    }

    /**
     * Uses the current shard allocation information to create a physical execution plan.
     * <br />
     * {@code limit}, {@code offset}, {@code order} can be passed from one operator to another. Depending on the
     * operators implementation. Operator may choose to make use of this information, but can also ignore it.
     */
    Plan build(Planner.Context plannerContext,
               ProjectionBuilder projectionBuilder,
               int limit,
               int offset,
               @Nullable OrderBy order,
               @Nullable Integer pageSizeHint);

    /**
     * Used to generate optimized operators.
     * E.g. Aggregate(count(*)) + Collect -> Count
     */
    LogicalPlan tryCollapse();

    List<Symbol> outputs();

    /**
     * Indicates if the operators which are added on top of this LogicalPlan should operate on a shard level.
     * Operating on a shard level increases parallelism.
     */
    default boolean preferShardProjections() {
        return false;
    }

    /**
     * A mapping from from symbol to symbol.
     * This is used across relation boundaries to map parent expression to source expression
     *
     * Example:
     * <pre>
     *     select tt.bb from
     *          (select t.b + t.b as bb from t) tt
     *
     * expressionMapping
     *      tt.bb -> t.b + t.b
     * </pre>
     */
    Map<Symbol, Symbol> expressionMapping();

    List<AbstractTableRelation> baseTables();
}
