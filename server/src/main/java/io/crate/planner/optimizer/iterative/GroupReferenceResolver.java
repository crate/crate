/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.optimizer.iterative;

import java.util.function.Function;
import java.util.function.UnaryOperator;

import io.crate.common.collections.Lists;
import io.crate.planner.operators.LogicalPlan;

/**
 * The GroupReferenceResolver resolves a GroupReference to the referenced LogicalPlan
 */
public interface GroupReferenceResolver extends UnaryOperator<LogicalPlan> {

    static GroupReferenceResolver from(Function<GroupReference, LogicalPlan> resolver) {
        return node -> {
            if (node instanceof GroupReference groupRef) {
                return resolver.apply(groupRef);
            }
            throw new IllegalStateException("Node is not a GroupReference");
        };
    }

    static LogicalPlan resolveFully(Function<LogicalPlan, LogicalPlan> resolver, LogicalPlan plan) {
        if (plan instanceof GroupReference g) {
            plan = resolver.apply(g);
        }

        if (plan.sources().isEmpty()) {
            return plan;
        }

        var sources = Lists.map(plan.sources(), x -> resolveFully(resolver, x));
        return plan.replaceSources(sources);
    }
}
