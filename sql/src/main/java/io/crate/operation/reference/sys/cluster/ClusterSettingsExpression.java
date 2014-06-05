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

package io.crate.operation.reference.sys.cluster;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.reference.sys.SysClusterObjectReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

public class ClusterSettingsExpression extends SysClusterObjectReference<Object> {

    public static final String NAME = "settings";

    abstract class SettingExpression extends SysClusterExpression<Object> {
        protected SettingExpression(String name) {
            super(new ColumnIdent(NAME, ImmutableList.of(name)));
        }
    }

    class ApplySettings implements NodeSettingsService.Listener {

        @Override
        public void onRefreshSettings(Settings settings) {
            final int newJobsLogSize = CrateSettings.JOBS_LOG_SIZE.extract(settings);
            if (newJobsLogSize != ClusterSettingsExpression.this.jobsLogSize) {
                logger.info("updating [{}] from [{}] to [{}]", CrateSettings.JOBS_LOG_SIZE.name(),
                        ClusterSettingsExpression.this.jobsLogSize, newJobsLogSize);
                ClusterSettingsExpression.this.jobsLogSize = newJobsLogSize;
            }
            final int newOperationsLogSize = CrateSettings.OPERATIONS_LOG_SIZE.extract(settings);
            if (newOperationsLogSize != ClusterSettingsExpression.this.operationsLogSize) {
                logger.info("updating [{}] from [{}] to [{}]", CrateSettings.OPERATIONS_LOG_SIZE.name(),
                        ClusterSettingsExpression.this.operationsLogSize, newOperationsLogSize);
                ClusterSettingsExpression.this.operationsLogSize = newOperationsLogSize;
            }

            final boolean newCollectStats = CrateSettings.COLLECT_STATS.extract(settings);
            if (newCollectStats != ClusterSettingsExpression.this.collectStats) {
                logger.info("{} [{}]",
                        (newCollectStats ? "enabling" : "disabling"),
                        CrateSettings.COLLECT_STATS.name());
                ClusterSettingsExpression.this.collectStats = newCollectStats;
            }

        }
    }

    protected final ESLogger logger;
    private volatile int jobsLogSize = CrateSettings.JOBS_LOG_SIZE.defaultValue();
    private volatile int operationsLogSize = CrateSettings.OPERATIONS_LOG_SIZE.defaultValue();
    private volatile boolean collectStats = CrateSettings.COLLECT_STATS.defaultValue();

    @Inject
    public ClusterSettingsExpression(Settings settings, NodeSettingsService nodeSettingsService) {
        super(NAME);
        this.logger = Loggers.getLogger(getClass(), settings);
        nodeSettingsService.addListener(new ApplySettings());
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(
                CrateSettings.JOBS_LOG_SIZE.name(),
                new SettingExpression(CrateSettings.JOBS_LOG_SIZE.name()) {

            @Override
            public Integer value() {
                return jobsLogSize;
            }
        });
        childImplementations.put(
                CrateSettings.OPERATIONS_LOG_SIZE.name(),
                new SettingExpression(CrateSettings.OPERATIONS_LOG_SIZE.name()) {
            @Override
            public Integer value() {
                return operationsLogSize;
            }
        });
        childImplementations.put(
                CrateSettings.COLLECT_STATS.name(),
                new SettingExpression(CrateSettings.COLLECT_STATS.name()) {
            @Override
            public Boolean value() {
                return collectStats;
            }
        });
    }
}
