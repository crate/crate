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

package io.crate.parquet;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static io.crate.testing.TestingHelpers.printedTable;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.Literal;
import io.crate.fdw.ServersMetadata.Server;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.types.DataTypes;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationMetadata;
import io.crate.metadata.RelationName;
import io.crate.role.Role;
import io.crate.role.metadata.RolesHelper;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class ParquetForeignDataWrapperTest extends CrateDummyClusterServiceUnitTest {
    private SQLExecutor e;

    @Before
    public void prepare() throws Exception {
        e = SQLExecutor.of(clusterService)
                .addTable("create table doc.taxi (trip_distance double, passenger_count bigint)");
    }

    @Test
    public void test_evaluates_references() throws Exception {
        Role role = RolesHelper.userOf("max");
        NodeContext nodeCtx = createNodeContext(List.of(role));
        ParquetForeignDataWrapper fdw = new ParquetForeignDataWrapper(Settings.EMPTY, new InputFactory(nodeCtx),
                Runnable::run);
        Settings options = Settings.builder()
                .put("inputfiles", List.of(Paths.get(
                        getClass().getResource("/data").toURI())
                        .resolve("yellow_tripdata_2026-01.parquet")
                        .toString()))
                .build();
        Server server = new Server("self", "parquet", "crate", Map.of(), options);
        CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        RelationName relationName = new RelationName("doc", "taxi");
        Reference nameRef = new SimpleReference(
                relationName,
                ColumnIdent.of("passenger_count"),
                RowGranularity.DOC,
                DataTypes.LONG,
                1,
                null);
        Map<ColumnIdent, Reference> references = Map.of(nameRef.column(), nameRef);
        RelationMetadata.ForeignTable foreignTable = new RelationMetadata.ForeignTable(relationName, references,
                server.name(), Settings.EMPTY);

        CompletableFuture<BatchIterator<Row>> it = fdw.getIterator(role, server, foreignTable, txnCtx,
                List.of(e.asSymbol("passenger_count * 2")),
                Literal.BOOLEAN_TRUE);
        List<Row> rows = Utils.getRows(it.get());
        assertThat(printedTable(rows)).isEqualTo("2\n0\n0\n8\n0\n4\n2\n0\n2\n6\n");
    }
}
