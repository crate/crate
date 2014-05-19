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
import io.crate.operation.reference.sys.SysClusterObjectReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

public class ClusterSettingsExpression extends SysClusterObjectReference<Object> {

    public static final String NAME = "settings";

    public static final String JOBS_LOG_SIZE = "jobs_log_size";
    public static final String OPERATIONS_LOG_SIZE = "operations_log_size";

    public static final String SETTING_JOBS_LOG_SIZE = "cluster.jobs_log_size";
    public static final String SETTING_OPERATIONS_LOG_SIZE = "cluster.operations_log_size";

    abstract class SettingExpression extends SysClusterExpression<Object> {
        protected SettingExpression(String name) {
            super(new ColumnIdent(NAME, ImmutableList.of(name)));
        }
    }

    class ApplySettings implements NodeSettingsService.Listener {

        @Override
        public void onRefreshSettings(Settings settings) {
            final int newJobsLogSize = settings.getAsInt(SETTING_JOBS_LOG_SIZE,
                    ClusterSettingsExpression.this.jobsLogSize);
            if (newJobsLogSize != ClusterSettingsExpression.this.jobsLogSize) {
                logger.info("updating [{}] from [{}] to [{}]", SETTING_JOBS_LOG_SIZE,
                        ClusterSettingsExpression.this.jobsLogSize, newJobsLogSize);
                ClusterSettingsExpression.this.jobsLogSize = newJobsLogSize;
            }

            final int newOperationsLogSize = settings.getAsInt(SETTING_OPERATIONS_LOG_SIZE,
                    ClusterSettingsExpression.this.operationsLogSize);
            if (newOperationsLogSize != ClusterSettingsExpression.this.operationsLogSize) {
                logger.info("updating [{}] from [{}] to [{}]", SETTING_OPERATIONS_LOG_SIZE,
                        ClusterSettingsExpression.this.operationsLogSize, newOperationsLogSize);
                ClusterSettingsExpression.this.operationsLogSize = newOperationsLogSize;
            }

        }
    }

    protected final ESLogger logger;
    private volatile int jobsLogSize = 0;
    private volatile int operationsLogSize = 0;

    @Inject
    public ClusterSettingsExpression(Settings settings, NodeSettingsService nodeSettingsService) {
        super(NAME);
        this.logger = Loggers.getLogger(getClass(), settings);
        nodeSettingsService.addListener(new ApplySettings());
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(JOBS_LOG_SIZE, new SettingExpression(JOBS_LOG_SIZE) {
            @Override
            public String value() {
                return String.valueOf(jobsLogSize);
            }
        });
        childImplementations.put(OPERATIONS_LOG_SIZE, new SettingExpression(OPERATIONS_LOG_SIZE) {
            @Override
            public String value() {
                return String.valueOf(operationsLogSize);
            }
        });
    }
}
