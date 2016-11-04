/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.node;

import io.crate.planner.node.dql.*;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.node.fetch.FetchPhase;
import org.elasticsearch.common.io.stream.Streamable;

import java.util.Collection;

public interface ExecutionPhase extends Streamable {

    String DIRECT_RETURN_DOWNSTREAM_NODE = "_response";

    int NO_EXECUTION_PHASE = Integer.MAX_VALUE;

    interface ExecutionPhaseFactory<T extends ExecutionPhase> {
        T create();
    }

    enum Type {
        COLLECT(RoutedCollectPhase.FACTORY),
        COUNT(CountPhase.FACTORY),
        FILE_URI_COLLECT(FileUriCollectPhase.FACTORY),
        MERGE(MergePhase.FACTORY),
        FETCH(FetchPhase.FACTORY),
        NESTED_LOOP(NestedLoopPhase.FACTORY),
        UNION(UnionPhase.FACTORY),
        TABLE_FUNCTION_COLLECT(TableFunctionCollectPhase.FACTORY);

        private final ExecutionPhaseFactory factory;

        Type(ExecutionPhaseFactory factory) {
            this.factory = factory;
        }

        public ExecutionPhaseFactory factory() {
            return factory;
        }
    }

    Type type();

    String name();

    int phaseId();

    Collection<String> nodeIds();

    <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context);
}
