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

package io.crate.execution.dsl.phases;


import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;

public interface ExecutionPhase extends Writeable {

    String DIRECT_RESPONSE = "_response";
    List<String> DIRECT_RESPONSE_LIST = Collections.singletonList("_response");

    int NO_EXECUTION_PHASE = Integer.MAX_VALUE;


    enum Type {
        COLLECT(RoutedCollectPhase::new),
        COUNT(CountPhase::new),
        FILE_URI_COLLECT(FileUriCollectPhase::new),
        MERGE(MergePhase::new),
        FETCH(FetchPhase::new),
        NESTED_LOOP(NestedLoopPhase::new),
        HASH_JOIN(HashJoinPhase::new),
        TABLE_FUNCTION_COLLECT(in -> {
            throw new UnsupportedOperationException("TableFunctionCollectPhase is not streamable"); }),
        PKLookup(PKLookupPhase::new),
        FOREIGN_COLLECT(ForeignCollectPhase::new);

        public static final List<Type> VALUES = List.of(values());

        private final Writeable.Reader<ExecutionPhase> reader;

        Type(Writeable.Reader<ExecutionPhase> reader) {
            this.reader = reader;
        }

        public ExecutionPhase fromStream(StreamInput in) throws IOException {
            return reader.read(in);
        }
    }

    Type type();

    String name();

    int phaseId();

    Collection<String> nodeIds();

    <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context);

    default String label() {
        return name() + ": " + phaseId();
    }
}
