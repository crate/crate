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

package io.crate.expression.reference.sys.check.cluster;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.ClusterReferenceResolver;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.expression.reference.sys.check.SysCheck.Severity;
import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SysChecksTest extends CrateUnitTest {

    private final ClusterService clusterService = mock(ClusterService.class);
    private final ClusterReferenceResolver referenceResolver = mock(ClusterReferenceResolver.class);
    private final SchemaInfo docSchemaInfo = mock(DocSchemaInfo.class);
    private final DocTableInfo docTableInfo = mock(DocTableInfo.class);

    @SuppressWarnings("unchecked")
    @Test
    public void testNumberOfPartitionCorrectPartitioning() {
        NumberOfPartitionsSysCheck numberOfPartitionsSysCheck = new NumberOfPartitionsSysCheck(
            mock(Schemas.class));

        when(docSchemaInfo.getTables()).thenReturn(ImmutableList.of(docTableInfo, docTableInfo));
        when(docTableInfo.isPartitioned()).thenReturn(true);

        List<PartitionName> partitionsFirst = buildPartitions(500);
        List<PartitionName> partitionsSecond = buildPartitions(100);
        when(docTableInfo.partitions()).thenReturn(partitionsFirst, partitionsSecond);

        assertThat(numberOfPartitionsSysCheck.id(), is(2));
        assertThat(numberOfPartitionsSysCheck.severity(), is(Severity.MEDIUM));
        assertThat(numberOfPartitionsSysCheck.validateDocTablesPartitioning(docSchemaInfo), is(true));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNumberOfPartitionsWrongPartitioning() {
        NumberOfPartitionsSysCheck numberOfPartitionsSysCheck = new NumberOfPartitionsSysCheck(mock(Schemas.class));
        List<PartitionName> partitions = buildPartitions(1001);

        when(docSchemaInfo.getTables()).thenReturn(ImmutableList.of(docTableInfo));
        when(docTableInfo.isPartitioned()).thenReturn(true);
        when(docTableInfo.partitions()).thenReturn(partitions);

        assertThat(numberOfPartitionsSysCheck.id(), is(2));
        assertThat(numberOfPartitionsSysCheck.severity(), is(Severity.MEDIUM));
        assertThat(numberOfPartitionsSysCheck.validateDocTablesPartitioning(docSchemaInfo), is(false));
    }

    private List<PartitionName> buildPartitions(int size) {
        List<PartitionName> partitions = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            partitions.add(new PartitionName(new RelationName("doc", "partition-" + i), Collections.emptyList()));
        }

        return partitions;
    }

}
