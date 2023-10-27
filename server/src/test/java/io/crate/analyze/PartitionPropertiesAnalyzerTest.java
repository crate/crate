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

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;

import org.junit.Test;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class PartitionPropertiesAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private PartitionName getPartitionName(DocTableInfo tableInfo) {
        return PartitionPropertiesAnalyzer.toPartitionName(
            tableInfo,
            Collections.singletonList(new Assignment<>(new QualifiedName("name"), "foo"))
        );
    }

    @Test
    public void testPartitionNameFromAssignmentWithBytesRef() {
        DocTableInfo tableInfo = SQLExecutor.partitionedTableInfo(
            new RelationName("doc", "users"),
            "create table doc.users (name text primary key) partitioned by (name)",
            clusterService);

        PartitionName partitionName = getPartitionName(tableInfo);
        assertThat(partitionName.values()).containsExactly("foo");
        assertThat(partitionName.asIndexName()).isEqualTo(".partitioned.users.0426crrf");
    }

    @Test
    public void testPartitionNameOnRegularTable() {
        DocTableInfo tableInfo = SQLExecutor.tableInfo(
            new RelationName("doc", "users"),
            "create table doc.users (name text primary key)",
            clusterService);

        assertThatThrownBy(() -> getPartitionName(tableInfo))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("table 'doc.users' is not partitioned");
    }
}
