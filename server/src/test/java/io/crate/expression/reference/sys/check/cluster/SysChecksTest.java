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

package io.crate.expression.reference.sys.check.cluster;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.expression.reference.sys.check.SysCheck.Severity;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.SchemaInfo;

public class SysChecksTest extends ESTestCase {

    private final Schemas schemas = mock(Schemas.class);
    private final SchemaInfo docSchemaInfo = mock(DocSchemaInfo.class);
    private final DocTableInfo docTableInfo = mock(DocTableInfo.class);
    private final ClusterService clusterService = mock(ClusterService.class);

    @SuppressWarnings("unchecked")
    @Test
    public void testNumberOfPartitionCorrectPartitioning() {
        NumberOfPartitionsSysCheck numberOfPartitionsSysCheck = new NumberOfPartitionsSysCheck(
            schemas,
            clusterService);

        when(schemas.iterator()).thenReturn(List.of(docSchemaInfo).iterator());
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        when(docSchemaInfo.getTables()).thenReturn(List.of(docTableInfo, docTableInfo));
        when(docTableInfo.isPartitioned()).thenReturn(true);

        List<PartitionName> partitionsFirst = buildPartitions(500);
        List<PartitionName> partitionsSecond = buildPartitions(100);
        when(docTableInfo.getPartitions(any())).thenReturn(partitionsFirst, partitionsSecond);

        assertThat(numberOfPartitionsSysCheck.id(), is(2));
        assertThat(numberOfPartitionsSysCheck.severity(), is(Severity.MEDIUM));
        assertThat(numberOfPartitionsSysCheck.isValid(), is(true));
    }

    @Test
    public void testNumberOfPartitionsWrongPartitioning() {
        NumberOfPartitionsSysCheck numberOfPartitionsSysCheck = new NumberOfPartitionsSysCheck(
            schemas,
            clusterService);
        List<PartitionName> partitions = buildPartitions(1001);

        when(schemas.iterator()).thenReturn(List.of(docSchemaInfo).iterator());
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        when(docSchemaInfo.getTables()).thenReturn(List.of(docTableInfo));
        when(docTableInfo.isPartitioned()).thenReturn(true);
        when(docTableInfo.getPartitions(any())).thenReturn(partitions);

        assertThat(numberOfPartitionsSysCheck.id(), is(2));
        assertThat(numberOfPartitionsSysCheck.severity(), is(Severity.MEDIUM));
        assertThat(numberOfPartitionsSysCheck.isValid(), is(false));
    }

    private List<PartitionName> buildPartitions(int size) {
        List<PartitionName> partitions = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            partitions.add(new PartitionName(new RelationName("doc", "partition-" + i), Collections.emptyList()));
        }

        return partitions;
    }

}
