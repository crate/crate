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

package io.crate.metadata.rule.ingest;

import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IngestionServiceTest extends CrateDummyClusterServiceUnitTest {

    private static final String UNDER_TEST_SOURCE_IDENT = "mqtt";
    private static final String UNDER_TEST_RULE_TARGET_TABLE = "temp_data.raw";
    private static final IngestRule UNDER_TEST_INGEST_RULE =
        new IngestRule("temperature", UNDER_TEST_RULE_TARGET_TABLE, "(topic like 'temperature/%')");
    private static final IngestRule HUMIDITY_INGEST_RULE =
        new IngestRule("humidity", "humidity_data.raw", "(topic like 'humidity/%')");

    private Schemas schemas;
    private IngestRulesMetaData ingestRulesMetaData;
    private IngestionService ingestionService;

    @Before
    public void setupIngestionServiceAndIngestRulesMetaData() {
        schemas = mock(Schemas.class);
        ingestionService = new IngestionService(schemas, clusterService);
        ImmutableOpenMap.Builder<String, MetaData.Custom> customsBuilder = ImmutableOpenMap.builder();
        ingestRulesMetaData = new IngestRulesMetaData(new HashMap<>());
        customsBuilder.put(IngestRulesMetaData.TYPE, ingestRulesMetaData);
        ImmutableOpenMap<String, MetaData.Custom> customs = customsBuilder.build();

        MetaData metaData = MetaData.builder().customs(customs).build();
        ClusterState clusterState = ClusterState.builder(clusterService.state())
            .metaData(metaData)
            .build();

        ClusterServiceUtils.setState(clusterService, clusterState);
        setupIngestRules();
    }

    private void setupIngestRules() {
        ingestRulesMetaData.createIngestRule(UNDER_TEST_SOURCE_IDENT, UNDER_TEST_INGEST_RULE);
        ingestRulesMetaData.createIngestRule("kafka", HUMIDITY_INGEST_RULE);
    }

    @Test
    public void testRegisterImplementationAppliesCurrentRules() {
        final AtomicReference<Set<IngestRule>> receivedRules = new AtomicReference<>();
        ingestionService.registerImplementation(UNDER_TEST_SOURCE_IDENT,
            rules -> receivedRules.set(rules));
        assertThat(receivedRules.get().size(), is(1));
        assertThat(receivedRules.get().iterator().next(), is(UNDER_TEST_INGEST_RULE));
    }

    @Test
    public void testOnClusterStateChangeRulesAreRevalidatedAndPushedToListener() {
        when(schemas.tableExists(any())).thenAnswer(invocationOnMock -> {
            TableIdent tableIdent = (TableIdent) invocationOnMock.getArguments()[0];
            // simulate target table was dropped
            if (tableIdent.fqn().equals(UNDER_TEST_RULE_TARGET_TABLE)) {
                return false;
            } else {
                return true;
            }
        });

        final AtomicReference<Set<IngestRule>> receivedRules = new AtomicReference<>();
        ingestionService.registerImplementation(UNDER_TEST_SOURCE_IDENT, rules -> receivedRules.set(rules));
        ingestionService.clusterChanged(new ClusterChangedEvent("table_dropped", newClusterState(), clusterService.state()));
        assertThat(receivedRules.get(), notNullValue());
        assertThat(receivedRules.get().size(), is(0));
    }

    private ClusterState newClusterState() {
        MetaData newMetaData = MetaData.builder(clusterService.state().metaData()).build();
        return ClusterState.builder(clusterService.state())
            .metaData(newMetaData)
            .build();
    }
}
