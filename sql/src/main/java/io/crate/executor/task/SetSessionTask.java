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
import io.crate.data.BatchConsumer;
import io.crate.data.Row;
import io.crate.data.RowsBatchIterator;
import io.crate.executor.JobTask;
import io.crate.metadata.settings.session.SessionSettingApplier;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.sql.tree.Expression;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SetSessionTask extends JobTask {

    private static final ESLogger LOGGER = Loggers.getLogger(SetSessionTask.class);

    private final SessionContext sessionContext;
    private final Map<String, List<Expression>> settings;

    public SetSessionTask(UUID jobId, Map<String, List<Expression>> settings, SessionContext sessionContext) {
        super(jobId);
        this.sessionContext = sessionContext;
        this.settings = settings;
    }

    @Override
    public void execute(BatchConsumer consumer, Row parameters) {
        for (Map.Entry<String, List<Expression>> setting : settings.entrySet()) {
            SessionSettingApplier applier = SessionSettingRegistry.getApplier(setting.getKey());
            if (applier != null) {
                // `search_path` is the only session setting currently supported.
                // Possible variations of the setting values that might cause an exception
                // are restricted by the parser. Therefore, for now we do not handle
                // exceptions here, e.g. by calling fail on the upstream (rowReceiver.fail(...))
                applier.apply(parameters, setting.getValue(), sessionContext);
            } else {
                LOGGER.warn("SET SESSION STATEMENT WILL BE IGNORED: {}", setting);
            }
        }
        consumer.accept(RowsBatchIterator.empty(), null);
    }
}

