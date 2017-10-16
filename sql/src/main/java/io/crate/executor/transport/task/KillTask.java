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

package io.crate.executor.transport.task;

import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.executor.JobTask;
import io.crate.executor.transport.OneRowActionListener;
import io.crate.executor.transport.kill.KillAllRequest;
import io.crate.executor.transport.kill.TransportKillAllNodeAction;

import java.util.UUID;

public class KillTask extends JobTask {

    private final TransportKillAllNodeAction nodeAction;

    public KillTask(TransportKillAllNodeAction nodeAction, UUID jobId) {
        super(jobId);
        this.nodeAction = nodeAction;
    }

    @Override
    public void execute(RowConsumer consumer, Row parameters) {
        nodeAction.broadcast(new KillAllRequest(), new OneRowActionListener<>(consumer, Row1::new));
    }
}
