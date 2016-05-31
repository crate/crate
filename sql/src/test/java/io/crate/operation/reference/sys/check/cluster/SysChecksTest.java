/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.operation.reference.sys.check.cluster;

import io.crate.metadata.NestedReferenceResolver;
import io.crate.metadata.PartitionName;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.table.SchemaInfo;
import io.crate.operation.reference.sys.check.SysCheck.Severity;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.ClusterService;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SysChecksTest extends CrateUnitTest {

    private final ClusterService clusterService = mock(ClusterService.class);
    private final NestedReferenceResolver referenceResolver = mock(NestedReferenceResolver.class);
    private final Iterator docSchemaInfoItr = mock(Iterator.class);
    private final SchemaInfo docSchemaInfo = mock(DocSchemaInfo.class);
    private final DocTableInfo docTableInfo = mock(DocTableInfo.class);

    @Test
    public void testMaxMasterNodesCheckWithEmptySetting() {
        MinMasterNodesSysCheck minMasterNodesCheck = new MinMasterNodesSysCheck(clusterService, referenceResolver);

        assertThat(minMasterNodesCheck.id(), is(1));
        assertThat(minMasterNodesCheck.severity(), is(Severity.HIGH));
        assertThat(minMasterNodesCheck.validate(2,
            CrateSettings.GATEWAY_RECOVERY_AFTER_NODES.defaultValue()), is(false));
    }

    @Test
    public void testMaxMasterNodesCheckWithCorrectSetting() {
        MinMasterNodesSysCheck minMasterNodesCheck = new MinMasterNodesSysCheck(clusterService, referenceResolver);

        assertThat(minMasterNodesCheck.id(), is(1));
        assertThat(minMasterNodesCheck.severity(), is(Severity.HIGH));
        assertThat(minMasterNodesCheck.validate(8, 5), is(true));
    }

    @Test
    public void testMaxMasterNodesCheckWithLessThanQuorum() {
        MinMasterNodesSysCheck minMasterNodesCheck = new MinMasterNodesSysCheck(clusterService, referenceResolver);

        assertThat(minMasterNodesCheck.id(), is(1));
        assertThat(minMasterNodesCheck.severity(), is(Severity.HIGH));
        assertThat(minMasterNodesCheck.validate(6, 3), is(false));
    }

    @Test
    public void testMaxMasterNodesCheckWithGreaterThanNodes() {
        MinMasterNodesSysCheck minMasterNodesCheck = new MinMasterNodesSysCheck(clusterService, referenceResolver);

        assertThat(minMasterNodesCheck.id(), is(1));
        assertThat(minMasterNodesCheck.severity(), is(Severity.HIGH));
        assertThat(minMasterNodesCheck.validate(6, 7), is(false));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNumberOfPartitionCorrectPartitioning() {
        NumberOfPartitionsSysCheck numberOfPartitionsSysCheck = new NumberOfPartitionsSysCheck(
            mock(ReferenceInfos.class));

        when(docSchemaInfo.iterator()).thenReturn(docSchemaInfoItr);
        when(docSchemaInfoItr.hasNext()).thenReturn(true, true, false);
        when(docSchemaInfoItr.next()).thenReturn(docTableInfo);
        when(docTableInfo.isPartitioned()).thenReturn(true, true);

        List<PartitionName> partitionsFirst = mock(List.class);
        List<PartitionName> partitionsSecond = mock(List.class);
        when(docTableInfo.partitions()).thenReturn(partitionsFirst, partitionsSecond);
        when(partitionsFirst.size()).thenReturn(500);
        when(partitionsSecond.size()).thenReturn(100);

        assertThat(numberOfPartitionsSysCheck.id(), is(2));
        assertThat(numberOfPartitionsSysCheck.severity(), is(Severity.MEDIUM));
        assertThat(numberOfPartitionsSysCheck.validateDocTablesPartitioning(docSchemaInfo), is(true));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNumberOfPartitionsWrongPartitioning() {
        NumberOfPartitionsSysCheck numberOfPartitionsSysCheck = new NumberOfPartitionsSysCheck(
            mock(ReferenceInfos.class));
        List<PartitionName> partitions = mock(List.class);

        when(docSchemaInfo.iterator()).thenReturn(docSchemaInfoItr);
        when(docSchemaInfoItr.hasNext()).thenReturn(true, false);
        when(docSchemaInfoItr.next()).thenReturn(docTableInfo);
        when(docTableInfo.isPartitioned()).thenReturn(true);
        when(docTableInfo.partitions()).thenReturn(partitions);
        when(partitions.size()).thenReturn(1001);

        assertThat(numberOfPartitionsSysCheck.id(), is(2));
        assertThat(numberOfPartitionsSysCheck.severity(), is(Severity.MEDIUM));
        assertThat(numberOfPartitionsSysCheck.validateDocTablesPartitioning(docSchemaInfo), is(false));
    }

}
