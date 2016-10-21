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

package io.crate.executor.task;

import io.crate.action.sql.SessionContext;
import io.crate.executor.JobTask;
import io.crate.metadata.settings.session.SessionSettingApplier;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.RowReceiver;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.UUID;

public class SetSessionTask extends JobTask {

    private static final ESLogger LOGGER = Loggers.getLogger(SetSessionTask.class);

    private final SessionContext sessionContext;
    private final Settings settings;

    public SetSessionTask(UUID jobId, Settings settings, SessionContext sessionContext) {
        super(jobId);
        this.sessionContext = sessionContext;
        this.settings = settings;
    }

    @Override
    public void execute(final RowReceiver rowReceiver) {
        for (String setting : settings.names()) {
            SessionSettingApplier applier = SessionSettingRegistry.getApplier(setting);
            if (applier != null) {
                applier.apply(settings.get(setting), sessionContext);
            } else {
                LOGGER.warn("SET LOCAL STATEMENTS WILL BE IGNORED: {}", setting);
            }
            rowReceiver.finish(RepeatHandle.UNSUPPORTED);
        }
    }
}

