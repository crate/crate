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

package io.crate.integrationtests;

import io.crate.action.sql.SQLActionException;
import io.crate.metadata.rule.ingest.IngestRule;
import io.crate.metadata.rule.ingest.IngestRulesMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.Matchers.is;

public class IngestRulesIntegrationTest extends SQLTransportIntegrationTest {

    private static final String INGEST_RULE_NAME = "testingestrule";

    @Before
    public void setupTableAndIngestRule() {
        execute("create table t1 (id int)");
        execute("create ingest rule " + INGEST_RULE_NAME + " on mqtt where topic = 'test' into t1");
    }

    @After
    public void dropTableAndIngestRule() {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        MetaData metaData = clusterService.state().metaData();
        IngestRulesMetaData ingestRulesMetaData = (IngestRulesMetaData) metaData.getCustoms().get(IngestRulesMetaData.TYPE);
        Set<IngestRule> allRules = ingestRulesMetaData.getAllRulesForTargetTable("doc.t1");
        for (IngestRule rule : allRules) {
            execute("drop ingest rule if exists " + rule.getName());
        }
    }

    @Test
    public void testCreateRule() {
        execute("create ingest rule test on mqtt where topic = 'test' into t1");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testCreateExistingRuleFails() {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(
            "SQLParseException: Ingest rule with name " + INGEST_RULE_NAME + " already exists");
        execute("create ingest rule " + INGEST_RULE_NAME + " on mqtt where topic = 'test' into t1");
    }

    @Test
    public void testDropRule() {
        execute("drop ingest rule " + INGEST_RULE_NAME);
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testDropMissingRuleFails() {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("ResourceNotFoundException: Ingest rule somerule doesn't exist");
        execute("drop ingest rule somerule");
    }

    @Test
    public void testDropMissingRuleIfExistsReturnsZeroAffectedRowsCount() {
        execute("drop ingest rule if exists somerule");
        assertThat(response.rowCount(), is(0L));
    }
}
