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

package io.crate.analyze;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;

import org.elasticsearch.cluster.metadata.Metadata;
import org.junit.Test;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class PartitionPropertiesAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testPartitionNameFromAssignmentsWithoutTable() {
        PartitionName partitionName = PartitionPropertiesAnalyzer.toPartitionName(
            new RelationName("dummy", "tbl"),
            List.of(
                new Assignment<>(new QualifiedName("p1"), 10),
                new Assignment<>(new QualifiedName("a"), 20)
            )
        );
        assertThat(partitionName.values()).containsExactly("10", "20");
        assertThat(partitionName.asIndexName()).isEqualTo("dummy..partitioned.tbl.081j2c0368o0");
    }

    @Test
    public void test_PartitionName_from_assignment_with_table() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addPartitionedTable(
                "create table doc.users (x int, p1 int) partitioned by (p1)",
                ".partitioned.users.041j2c0"
            )
            .build();

        Metadata metadata = clusterService.state().metadata();
        DocTableInfo tableInfo = e.resolveTableInfo("doc.users");
        PartitionName partitionName = PartitionPropertiesAnalyzer.createPartitionName(
            List.of(
                new Assignment<>(new QualifiedName("p1"), 10)
            ),
            tableInfo,
            metadata
        );
        assertThat(partitionName.values()).containsExactly("10");
        assertThat(partitionName.asIndexName()).isEqualTo(tableInfo.concreteIndices(metadata)[0]);

        assertThatThrownBy(() -> PartitionPropertiesAnalyzer.createPartitionName(
            List.of(new Assignment<>(new QualifiedName("p1"), 20)),
            tableInfo,
            metadata
        )).isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testPartitionNameOnRegularTable() {
        DocTableInfo tableInfo = SQLExecutor.tableInfo(
            new RelationName("doc", "users"),
            "create table doc.users (name text primary key)",
            clusterService);

        assertThatThrownBy(() -> PartitionPropertiesAnalyzer.toPartitionName(
            tableInfo,
            Collections.singletonList(new Assignment<>(new QualifiedName("name"), "foo"))
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("table 'doc.users' is not partitioned");
    }
}
