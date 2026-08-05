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

package io.crate.fdw;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.threadpool.ThreadPool.Names.SEARCH;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import io.crate.expression.InputFactory;
import io.crate.expression.symbol.Literal;
import io.crate.fdw.ServersMetadata.Server;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.role.Role;
import io.crate.role.metadata.RolesHelper;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;

public class JdbcForeignDataWrapperTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_cannot_access_localhost_as_regular_user() throws Exception {
        Role arthur = RolesHelper.userOf("arthur");
        NodeContext nodeCtx = createNodeContext(List.of(arthur));

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(SEARCH)).thenReturn(Runnable::run);

        var fdw = new JdbcForeignDataWrapper(Settings.EMPTY, new InputFactory(nodeCtx), threadPool);
        Settings options = Settings.builder()
            .put("url", "jdbc:postgresql://localhost:5432/")
            .build();
        Server server = new ServersMetadata.Server("self", "jdbc", "crate", Map.of(), options);
        CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        RelationName relationName = new RelationName("secret", "documents");
        Reference nameRef = new SimpleReference(
            relationName,
            ColumnIdent.of("name"),
            RowGranularity.DOC,
            DataTypes.STRING,
            1,
            null
        );
        Map<ColumnIdent, Reference> references = Map.of(nameRef.column(), nameRef);
        RelationMetadata.ForeignTable foreignTable = new RelationMetadata.ForeignTable(relationName, references, server.name(), Settings.EMPTY);
        assertThatThrownBy(() -> fdw.getIterator(arthur, server, foreignTable, txnCtx, List.of(nameRef), Literal.BOOLEAN_TRUE))
            .hasMessage("Only a super user can connect to localhost unless `fdw.allow_local` is set to true");
    }

    @Test
    public void test_can_access_remote_as_regular_user() throws Exception {
        Role arthur = RolesHelper.userOf("arthur");
        NodeContext nodeCtx = createNodeContext(List.of(arthur));

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(SEARCH)).thenReturn(Runnable::run);

        var fdw = new JdbcForeignDataWrapper(Settings.EMPTY, new InputFactory(nodeCtx), threadPool);
        Settings options = Settings.builder()
            .put("url", "jdbc:postgresql://192.0.2.0:5432/postgres")
            .build();
        Server server = new ServersMetadata.Server("self", "jdbc", "crate", Map.of(), options);
        CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        RelationName relationName = new RelationName("secret", "documents");
        Reference nameRef = new SimpleReference(
            relationName,
            ColumnIdent.of("name"),
            RowGranularity.DOC,
            DataTypes.STRING,
            1,
            null
        );
        Map<ColumnIdent, Reference> references = Map.of(nameRef.column(), nameRef);
        RelationMetadata.ForeignTable foreignTable = new RelationMetadata.ForeignTable(relationName, references, server.name(), Settings.EMPTY);
        // validates that no exception is thrown
        fdw.getIterator(arthur, server, foreignTable, txnCtx, List.of(nameRef), Literal.BOOLEAN_TRUE);
    }

    @Test
    public void test_get_stats_returns_empty_for_generic_jdbc_url() throws Exception {
        Role arthur = RolesHelper.userOf("arthur");
        NodeContext nodeCtx = createNodeContext(List.of(arthur));

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(SEARCH)).thenReturn(Runnable::run);

        var fdw = new JdbcForeignDataWrapper(Settings.EMPTY, new InputFactory(nodeCtx), threadPool);

        Settings options = Settings.builder()
            .put("url", "jdbc:mysql://192.0.2.0:3306/db")
            .build();
        Server server = new ServersMetadata.Server("self", "jdbc", "crate", Map.of(), options);
        CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        RelationName relationName = new RelationName("secret", "documents");
        RelationMetadata.ForeignTable foreignTable = new RelationMetadata.ForeignTable(relationName, Map.of(), server.name(), Settings.EMPTY);

        ForeignTableStats stats = fdw.getStats(arthur, server, foreignTable, txnCtx, List.of()).get();
        assertThat(stats).isEqualTo(ForeignTableStats.EMPTY);
    }
}
