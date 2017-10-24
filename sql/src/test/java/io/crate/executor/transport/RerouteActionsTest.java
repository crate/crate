/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.executor.transport;

import io.crate.analyze.RerouteAllocateReplicaShardAnalyzedStatement;
import io.crate.analyze.RerouteMoveShardAnalyzedStatement;
import io.crate.analyze.TableDefinitions;
import io.crate.data.Row;
import io.crate.metadata.TableIdent;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.StringLiteral;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;

public class RerouteActionsTest extends CrateUnitTest {

    public static final BlobTableInfo BLOB_TABLE_INFO = TableDefinitions.createBlobTable(
        new TableIdent(BlobSchemaInfo.NAME, "screenshots"));

    @Test
    public void testRerouteIndexOfBlobTable() throws Exception {
        RerouteMoveShardAnalyzedStatement statement = new RerouteMoveShardAnalyzedStatement(
            BLOB_TABLE_INFO,
            Collections.emptyList(),
            SqlParser.createExpression("0"),
            SqlParser.createExpression("node1"),
            SqlParser.createExpression("node2")
        );
        String index = RerouteActions.getRerouteIndex(statement, Row.EMPTY);
        assertThat(index, is("blob.screenshots"));
    }

    @Test
    public void testRerouteIndexOfPartedDocTable() throws Exception {
        RerouteMoveShardAnalyzedStatement statement = new RerouteMoveShardAnalyzedStatement(
            TableDefinitions.TEST_PARTITIONED_TABLE_INFO,
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
            TableDefinitions.TEST_PARTITIONED_TABLE_INFO,
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
            TableDefinitions.TEST_PARTITIONED_TABLE_INFO,
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
            TableDefinitions.USER_TABLE_INFO,
            Arrays.asList(new Assignment(
                new QualifiedNameReference(new QualifiedName("date")), new StringLiteral("1"))),
            SqlParser.createExpression("0"),
            SqlParser.createExpression("node1"),
            SqlParser.createExpression("node2")
        );

        RerouteActions.getRerouteIndex(statement, Row.EMPTY);
    }

    @Test
    public void testRerouteExecution() throws Exception {
        AtomicReference<ClusterRerouteRequest> reqRef = new AtomicReference<>();

        RerouteAllocateReplicaShardAnalyzedStatement statement = new RerouteAllocateReplicaShardAnalyzedStatement(
            TableDefinitions.USER_TABLE_INFO,
            Collections.emptyList(),
            SqlParser.createExpression("0"),
            SqlParser.createExpression("node1"));

        RerouteActions.execute((req, listener) -> reqRef.set(req), statement, Row.EMPTY);
        ClusterRerouteRequest actualRequest = reqRef.get();

        ClusterRerouteRequest request = new ClusterRerouteRequest();
        AllocateReplicaAllocationCommand command = new AllocateReplicaAllocationCommand("users", 0, "node1");
        request.add(command);
        assertEquals(request, actualRequest);
    }

    @Test
    public void testAllocateReplicaShardRequest() throws Exception {
        RerouteAllocateReplicaShardAnalyzedStatement statement = new RerouteAllocateReplicaShardAnalyzedStatement(
            TableDefinitions.USER_TABLE_INFO,
            Collections.emptyList(),
            SqlParser.createExpression("0"),
            SqlParser.createExpression("node1"));
        ClusterRerouteRequest actualRequest = RerouteActions.prepareRequest(statement, Row.EMPTY);

        ClusterRerouteRequest request = new ClusterRerouteRequest();
        AllocateReplicaAllocationCommand command = new AllocateReplicaAllocationCommand(
            TableDefinitions.USER_TABLE_INFO.ident().indexName(), 0, "node1");
        request.add(command);
        assertEquals(request, actualRequest);
    }

    @Test
    public void testMoveShardRequest() throws Exception {
        RerouteMoveShardAnalyzedStatement statement = new RerouteMoveShardAnalyzedStatement(
            TableDefinitions.USER_TABLE_INFO,
            Collections.EMPTY_LIST,
            SqlParser.createExpression("0"),
            SqlParser.createExpression("node1"),
            SqlParser.createExpression("node2"));
        ClusterRerouteRequest actualRequest = RerouteActions.prepareRequest(statement, Row.EMPTY);

        ClusterRerouteRequest request = new ClusterRerouteRequest();
        MoveAllocationCommand command = new MoveAllocationCommand(
            TableDefinitions.USER_TABLE_INFO.ident().indexName(), 0, "node1", "node2");
        request.add(command);
        assertEquals(request, actualRequest);
    }
}
