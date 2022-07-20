/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.ddl;

import static io.crate.metadata.PartitionName.templateName;
import static java.util.Collections.singletonMap;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.HashMap;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.Constants;
import io.crate.analyze.BoundAddColumn;
import io.crate.data.Row;
import io.crate.metadata.IndexMappings;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.ddl.AlterTableAddColumnPlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class TransportSchemaUpdateActionTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testTemplateMappingUpdateFailsIfTypeIsDifferent() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addPartitionedTable(
                "create table t (p int) partitioned by (p)"
            ).build();

        ClusterState currentState = clusterService.state();
        PlannerContext plannerContext = e.getPlannerContext(currentState);
        BoundAddColumn addXLong = AlterTableAddColumnPlan.bind(
            e.analyze("alter table t add column x long"),
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            Row.EMPTY,
            SubQueryResults.EMPTY,
            null
        );
        BoundAddColumn addXString = AlterTableAddColumnPlan.bind(
            e.analyze("alter table t add column x string"),
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            Row.EMPTY,
            SubQueryResults.EMPTY,
            null
        );

        String templateName = templateName("doc", "t");
        IndexTemplateMetadata template = currentState.metadata().templates().get(templateName);
        ClusterState stateWithXLong = ClusterState.builder(currentState)
            .metadata(Metadata.builder(currentState.metadata())
                .put(IndexTemplateMetadata.builder(templateName)
                    .patterns(template.patterns())
                    .putMapping(
                        Constants.DEFAULT_MAPPING_TYPE,
                        Strings.toString(JsonXContent.contentBuilder().map(addXLong.mapping()))))
                .build()
            ).build();

        expectedException.expect(IllegalArgumentException.class);
        TransportSchemaUpdateAction.updateTemplate(
            NamedXContentRegistry.EMPTY,
            stateWithXLong,
            templateName,
            addXString.mapping()
        );
    }

    @Test
    public void testDynamicTrueCanBeChangedFromBooleanToStringValue() {
        HashMap<String, Object> source = new HashMap<>();
        source.put("dynamic", true);
        TransportSchemaUpdateAction.mergeIntoSource(source, singletonMap("dynamic", "true"));
        assertThat(source.get("dynamic"), Matchers.is("true"));
    }

    @Test
    public void testMergeIntoSourceWithNullValuedSource() {
        HashMap<String, Object> source = new HashMap<>();
        source.put("dynamic", null);
        TransportSchemaUpdateAction.mergeIntoSource(source, singletonMap("dynamic", "true"));
        assertThat(source.get("dynamic"), Matchers.is("true"));
    }

    @Test
    public void testVersionChangesAreIgnored() {
        HashMap<String, Object> source = new HashMap<>();
        String versionKey = "cratedb";
        source.put(versionKey, 100);

        TransportSchemaUpdateAction.mergeIntoSource(
            source,
            singletonMap(versionKey, 200),
            Arrays.asList("default", "_meta", IndexMappings.VERSION_STRING, versionKey)
        );

        assertThat(source.get(versionKey), is(100));
    }
}
