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

import io.crate.action.sql.SQLActionException;
import io.crate.execution.engine.collect.stats.JobsLogService;
import io.crate.settings.SharedSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class UserSessionIntegrationTest extends BaseUsersIntegrationTest {

    private String nodeEnterpriseEnabled;
    private String nodeEnterpriseDisabled;

    @Before
    public void setUpNodesUsersAndPrivileges() throws Exception {
        nodeEnterpriseEnabled = internalCluster().startNode(Settings.builder()
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true));
        nodeEnterpriseDisabled = internalCluster().startNode(Settings.builder()
            .put(SharedSettings.ENTERPRISE_LICENSE_SETTING.getKey(), false)
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true));
        super.createSessions();
    }

    @Test
    public void testSystemExecutorUsesSuperuserSession() {
        systemExecute("select username from sys.jobs", "sys", nodeEnterpriseEnabled);
        assertThat(response.rows()[0][0], is("crate"));
    }

    @Test
    public void testSystemExecutorNullUser() {
        systemExecute("select username from sys.jobs", "sys", nodeEnterpriseDisabled);
        assertNull(response.rows()[0][0]);
    }

    @Test
    public void testQueryWithNullUserAndEnabledUserManagement() {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("MissingPrivilegeException: Missing privilege for user 'User `null` is not authorized to execute statement'");
        execute("select username from sys.jobs", null, createNullUserSession(nodeEnterpriseEnabled));
    }
}
