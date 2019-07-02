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

package io.crate.execution.ddl;

import io.crate.analyze.RerouteAllocateReplicaShardAnalyzedStatement;
import io.crate.analyze.RerouteCancelShardAnalyzedStatement;
import io.crate.analyze.RerouteMoveShardAnalyzedStatement;
import io.crate.data.Row;
import io.crate.metadata.RelationName;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.BooleanLiteral;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.GenericProperty;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.StringLiteral;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.Randomness;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static io.crate.analyze.TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION;
import static io.crate.analyze.TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS;
import static io.crate.analyze.TableDefinitions.USER_TABLE_DEFINITION;
import static org.hamcrest.Matchers.is;

public class RerouteActionsTest extends CrateDummyClusterServiceUnitTest {

    private BlobTableInfo blobTable;
    private DocTableInfo userTable;
    private DocTableInfo partedTable;

    @Before
    public void setupTable() throws Exception {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService, 2, Randomness.get())
            .addTable(USER_TABLE_DEFINITION)
            .addPartitionedTable(TEST_PARTITIONED_TABLE_DEFINITION, TEST_PARTITIONED_TABLE_PARTITIONS)
            .addBlobTable("create blob table screenshots")
            .build();

        userTable = sqlExecutor.schemas().getTableInfo(new RelationName("doc", "users"));
        partedTable = sqlExecutor.schemas().getTableInfo(new RelationName("doc", "parted"));
        blobTable = sqlExecutor.schemas().getTableInfo(new RelationName("blob", "screenshots"));
    }

    @Test
    public void testRerouteIndexOfBlobTable() throws Exception {
        RerouteMoveShardAnalyzedStatement statement = new RerouteMoveShardAnalyzedStatement(
            blobTable,
            Collections.emptyList(),
            SqlParser.createExpression("0"),
            SqlParser.createExpression("node1"),
            SqlParser.createExpression("node2")
        );
        String index = RerouteActions.getRerouteIndex(statement, Row.EMPTY);
        assertThat(index, is(".blob_screenshots"));
    }

    @Test
    public void testRerouteIndexOfPartedDocTable() throws Exception {
        RerouteMoveShardAnalyzedStatement statement = new RerouteMoveShardAnalyzedStatement(
            partedTable,
            Arrays.asList(new Assignment(
                new QualifiedNameReference(new QualifiedName("date")), new StringLiteral("1395874800000"))),
            SqlParser.createExpression("0"),
            SqlParser.createExpression("node1"),
            SqlParser.createExpression("node2")
        );
        String index = RerouteActions.getRerouteIndex(statement, Row.EMPTY);
        assertThat(index, is(".partitioned.parted.04732cpp6ks3ed1o60o30c1g"));
    }

    @Test
    public void testRerouteIndexOfPartedDocTableWithoutPartitionClause() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("table is partitioned however no partition clause has been specified");
        RerouteMoveShardAnalyzedStatement statement = new RerouteMoveShardAnalyzedStatement(
            partedTable,
            Collections.emptyList(),
            SqlParser.createExpression("0"),
            SqlParser.createExpression("node1"),
            SqlParser.createExpression("node2")
        );
        RerouteActions.getRerouteIndex(statement, Row.EMPTY);
    }

    @Test
    public void testRerouteMoveShardPartitionedTableUnknownPartition() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Referenced partition \".partitioned.parted.04132\" does not exist.");
        RerouteMoveShardAnalyzedStatement statement = new RerouteMoveShardAnalyzedStatement(
            partedTable,
            Arrays.asList(new Assignment(
                new QualifiedNameReference(new QualifiedName("date")), new StringLiteral("1"))),
            SqlParser.createExpression("0"),
            SqlParser.createExpression("node1"),
            SqlParser.createExpression("node2")
        );

        RerouteActions.getRerouteIndex(statement, Row.EMPTY);
    }

    @Test
    public void testRerouteMoveShardUnpartitionedTableWithPartitionClause() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("table 'doc.users' is not partitioned");
        RerouteMoveShardAnalyzedStatement statement = new RerouteMoveShardAnalyzedStatement(
            userTable,
            Arrays.asList(new Assignment(
                new QualifiedNameReference(new QualifiedName("date")), new StringLiteral("1"))),
            SqlParser.createExpression("0"),
            SqlParser.createExpression("node1"),
            SqlParser.createExpression("node2")
        );

        RerouteActions.getRerouteIndex(statement, Row.EMPTY);
    }

    @Test
    public void test_allocate_replica_shard_request_creation() throws Exception {
        RerouteAllocateReplicaShardAnalyzedStatement statement = new RerouteAllocateReplicaShardAnalyzedStatement(
            userTable,
            Collections.emptyList(),
            SqlParser.createExpression("0"),
            SqlParser.createExpression("n1"));

        ClusterRerouteRequest actualRequest = RerouteActions.prepareAllocateReplicaReq(
            statement, Row.EMPTY, clusterService.state().nodes());
        ClusterRerouteRequest request = new ClusterRerouteRequest();
        var command = new AllocateReplicaAllocationCommand(userTable.ident().indexNameOrAlias(), 0, "n1");
        request.add(command);
        assertEquals(request, actualRequest);
    }

    @Test
    public void testMoveShardRequest() throws Exception {
        RerouteMoveShardAnalyzedStatement statement = new RerouteMoveShardAnalyzedStatement(
            userTable,
            Collections.emptyList(),
            SqlParser.createExpression("0"),
            SqlParser.createExpression("n1"),
            SqlParser.createExpression("n2"));
        ClusterRerouteRequest actualRequest = RerouteActions.prepareMoveShardReq(
            statement, Row.EMPTY, clusterService.state().nodes());

        ClusterRerouteRequest request = new ClusterRerouteRequest();
        MoveAllocationCommand command = new MoveAllocationCommand(
            userTable.ident().indexNameOrAlias(), 0, "n1", "n2");
        request.add(command);
        assertEquals(request, actualRequest);
    }

    @Test
    public void testCancelShardRequest() throws Exception {
        GenericProperties properties = new GenericProperties();
        properties.add(new GenericProperty("allow_primary", BooleanLiteral.TRUE_LITERAL));
        RerouteCancelShardAnalyzedStatement statement = new RerouteCancelShardAnalyzedStatement(
            userTable,
            Collections.emptyList(),
            SqlParser.createExpression("0"),
            SqlParser.createExpression("n1"),
            properties);
        ClusterRerouteRequest actualRequest = RerouteActions.prepareCancelShardReq(
            statement, Row.EMPTY, clusterService.state().nodes());

        ClusterRerouteRequest request = new ClusterRerouteRequest();
        CancelAllocationCommand command = new CancelAllocationCommand(
            userTable.ident().indexNameOrAlias(), 0, "n1", true);
        request.add(command);
        assertEquals(request, actualRequest);
    }

    @Test
    public void testCancelShardRequestWithInvalidProperty() throws Exception {
        GenericProperties properties = new GenericProperties();
        properties.add(new GenericProperty("invalid", BooleanLiteral.FALSE_LITERAL));

        RerouteCancelShardAnalyzedStatement statement = new RerouteCancelShardAnalyzedStatement(
            userTable,
            Collections.emptyList(),
            SqlParser.createExpression("0"),
            SqlParser.createExpression("node1"),
            properties);
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("\"invalid\" is not a valid setting for CANCEL SHARD");
        RerouteActions.prepareCancelShardReq(statement, Row.EMPTY, clusterService.state().nodes());
    }
}
