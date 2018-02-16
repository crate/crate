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

package io.crate.execution.jobs;

import io.crate.concurrent.CompletionListenable;
import io.crate.execution.dsl.phases.JoinPhase;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;

class JoinContext extends AbstractExecutionSubContext implements DownstreamExecutionSubContext {

    private final JoinPhase joinPhase;

    @Nullable
    private final PageBucketReceiver leftBucketReceiver;

    @Nullable
    private final PageBucketReceiver rightBucketReceiver;

    JoinContext(Logger logger,
                JoinPhase joinPhase,
                CompletionListenable completionListenable,
                @Nullable PageBucketReceiver leftBucketReceiver,
                @Nullable PageBucketReceiver rightBucketReceiver) {
        super(joinPhase.phaseId(), logger);

        this.joinPhase = joinPhase;
        this.leftBucketReceiver = leftBucketReceiver;
        this.rightBucketReceiver = rightBucketReceiver;

        completionListenable.completionFuture()
            .whenComplete((Object result, Throwable t) -> close(t));
    }

    @Override
    public String name() {
        return joinPhase.name();
    }

    @Override
    protected void innerClose(@Nullable Throwable t) {
    }

    @Override
    protected void innerKill(@Nullable Throwable t) {
        // killed via PageDownstreamContexts or if they're not available the nestedLoop integrates
        // into the previous executionPhase
    }

    @Override
    public PageBucketReceiver getBucketReceiver(byte inputId) {
        assert inputId < 2 : "Only 0 and 1 inputId's supported";
        if (inputId == 0) {
            return leftBucketReceiver;
        }
        return rightBucketReceiver;
    }

    @Override
    public String toString() {
        return "JoinContext{" +
               "id=" + id() +
               ", leftCtx=" + leftBucketReceiver +
               ", rightCtx=" + rightBucketReceiver +
               ", closed=" + isClosed() +
               '}';
    }

}
