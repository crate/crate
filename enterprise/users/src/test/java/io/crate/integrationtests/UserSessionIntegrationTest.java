/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.integrationtests;

import io.crate.execution.engine.collect.stats.JobsLogService;
import io.crate.settings.SharedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class UserSessionIntegrationTest extends BaseUsersIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings settings = super.nodeSettings(nodeOrdinal);
        if (nodeOrdinal == 0) { // Enterprise enabled
            return Settings.builder().put(settings)
                .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true)
                .put(SharedSettings.ENTERPRISE_LICENSE_SETTING.getKey(), true).build();
        }
        // Enterprise disabled
        return Settings.builder().put(settings)
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true)
            .put(SharedSettings.ENTERPRISE_LICENSE_SETTING.getKey(), false).build();
    }

    @Test
    public void testSystemExecutorUsesSuperuserSession() {
        systemExecute("select username from sys.jobs", "sys", getNodeByEnterpriseNode(true));
        assertThat(response.rows()[0][0], is("crate"));
    }

    @Test
    public void testSystemExecutorNullUser() {
        systemExecute("select username from sys.jobs", "sys", getNodeByEnterpriseNode(false));
        assertThat(response.rows()[0][0], is("crate"));
    }

    private String getNodeByEnterpriseNode(boolean enterpriseEnabled) {
        if (enterpriseEnabled) {
            return internalCluster().getNodeNames()[0];
        }
        return internalCluster().getNodeNames()[1];
    }
}
