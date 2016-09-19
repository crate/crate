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

package io.crate.jobs;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.crate.operation.projectors.ListenableRowReceiver;
import io.crate.planner.node.dql.UnionPhase;
import org.elasticsearch.common.logging.ESLogger;

import javax.annotation.Nullable;
import java.util.Map;

public class UnionContext extends AbstractExecutionSubContext implements DownstreamExecutionSubContext {

    private final UnionPhase unionPhase;
    private final Map<Byte, PageDownstreamContext> contexts;

    public UnionContext(ESLogger logger, UnionPhase unionPhase,
                        Map<Byte, PageDownstreamContext> contexts,
                        ListenableRowReceiver targetRowReceiver) {
        super(unionPhase.phaseId(), logger);
        this.unionPhase = unionPhase;
        this.contexts = contexts;

        Futures.addCallback(targetRowReceiver.finishFuture(), new FutureCallback<Object>() {
            @Override
            public void onSuccess(@Nullable Object result) {
                future.close(null);
            }

            @Override
            public void onFailure(@Nullable Throwable t) {
                future.close(t);
            }
        });
    }

    @Nullable
    @Override
    public PageBucketReceiver getBucketReceiver(byte inputId) {
        return contexts.get(inputId);
    }

    @Override
    public String name() {
        return unionPhase.name();
    }

    @Override
    public int id() {
        return unionPhase.phaseId();
    }
}
