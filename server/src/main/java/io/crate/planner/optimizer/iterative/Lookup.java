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
import java.util.stream.Stream;

import io.crate.planner.operators.LogicalPlan;

public interface Lookup {

    default LogicalPlan resolve(LogicalPlan node) {
        if (node instanceof GroupReference) {
            return resolveGroup(node).findFirst().get();
        }
        return node;
    }

    Stream<LogicalPlan> resolveGroup(LogicalPlan node);

    static Lookup from(Function<GroupReference, Stream<LogicalPlan>> resolver) {
        return node -> {
            if (!(node instanceof GroupReference)) {
                throw new IllegalStateException("Node is not a GroupReference");
            }
            return resolver.apply((GroupReference) node);
        };
    }
}
