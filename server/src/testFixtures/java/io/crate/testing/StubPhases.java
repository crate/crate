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

package io.crate.testing;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;
import io.crate.execution.dsl.phases.ExecutionPhase;
import io.crate.execution.dsl.phases.ExecutionPhaseVisitor;
import io.crate.execution.dsl.phases.UpstreamPhase;
import io.crate.planner.distribution.DistributionInfo;

public class StubPhases {

    public static UpstreamPhase newUpstreamPhase(int phaseId, DistributionInfo distributionInfo, String... executionNodes) {
        return new UpstreamStubPhase(phaseId, ExecutionPhase.Type.COLLECT, distributionInfo, executionNodes);
    }

    public static ExecutionPhase newPhase(int phaseId, String... executionNodes) {
        return new StubPhase(phaseId, ExecutionPhase.Type.COLLECT, executionNodes);
    }

    static class StubPhase implements ExecutionPhase {

        private final int phaseId;
        private final Type type;
        private final List<String> executionNodes;

        public StubPhase(int phaseId, Type type, String... executionNodes) {
            this.phaseId = phaseId;
            this.type = type;
            this.executionNodes = Arrays.asList(executionNodes);
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public String name() {
            return "stub";
        }

        @Override
        public int phaseId() {
            return phaseId;
        }

        @Override
        public Collection<String> nodeIds() {
            return executionNodes;
        }

        @Override
        public Streamer<?>[] getStreamers() {
            return new Streamer[0];
        }

        @Override
        public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }
    }

    static class UpstreamStubPhase extends StubPhase implements UpstreamPhase {

        private DistributionInfo distributionInfo;

        public UpstreamStubPhase(int phaseId, Type type, DistributionInfo distributionInfo, String... executionNodes) {
            super(phaseId, type, executionNodes);
            this.distributionInfo = distributionInfo;
        }

        @Override
        public DistributionInfo distributionInfo() {
            return distributionInfo;
        }

        @Override
        public void distributionInfo(DistributionInfo distributionInfo) {
            this.distributionInfo = distributionInfo;
        }
    }
}
