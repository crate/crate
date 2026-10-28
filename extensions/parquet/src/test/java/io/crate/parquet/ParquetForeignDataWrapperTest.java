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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.exceptions.UnauthorizedException;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.Literal;
import io.crate.fdw.ServersMetadata.Server;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationMetadata;
import io.crate.role.Role;
import io.crate.role.metadata.RolesHelper;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class ParquetForeignDataWrapperTest extends CrateDummyClusterServiceUnitTest {
    private SQLExecutor e;
    private Server server;
    private RelationMetadata.ForeignTable foreignTable;
    private CoordinatorTxnCtx txnCtx;

    @Before
    public void prepare() throws Exception {
        Settings options = Settings.builder()
                .put("local_files", List.of(Paths.get(
                        getClass().getResource("/data").toURI())
                        .resolve("yellow_taxi_01.parquet")
                        .toString()))
                .build();
        e = SQLExecutor.of(clusterService)
                .addServer("pq", "parquet", "crate", options)
                .addForeignTable(
                        """
                        create foreign table doc.taxi
                        (passenger_count bigint,
                        car_sensor object(strict) as (
                            engine_temp int,
                            outdoor_temp object(strict) as (
                                unit text,
                                value int
                            )
                        ),
                        car_sensor_json_ignored_policy object(ignored),
                        car_sensor_json object
            ) server pq""");


        foreignTable = e.resolveTableInfo("doc.taxi");
        server = new Server("pq", "parquet", "crate", Map.of(), options);
        txnCtx = CoordinatorTxnCtx.systemTransactionContext();
    }


    @Test
    public void test_evaluates_references() throws Exception {
        Role role = Role.CRATE_USER;
        NodeContext nodeCtx = createNodeContext(List.of(role));
        ParquetForeignDataWrapper fdw = new ParquetForeignDataWrapper(new InputFactory(nodeCtx));
        BatchIterator<Row> it = fdw.getIterator(role, server, foreignTable, txnCtx,
                List.of(e.asSymbol("passenger_count * 2"), e.asSymbol("passenger_count")),
                Literal.BOOLEAN_TRUE).get();
        List<Object[]> rows = it.toList(Row::materialize).get(10, TimeUnit.SECONDS);
        assertThat(rows).containsExactly(
            new Object[]{2L, 1L},
            new Object[]{0L, 0L},
            new Object[]{0L, 0L},
            new Object[]{8L, 4L},
            new Object[]{0L, 0L},
            new Object[]{4L, 2L},
            new Object[]{2L, 1L},
            new Object[]{0L, 0L},
            new Object[]{2L, 1L},
            new Object[]{6L, 3L}
        );
    }

    @Test
    public void test_projects_nested_field_from_object_type_strict_policy() throws Exception {
        Role role = Role.CRATE_USER;
        NodeContext nodeCtx = createNodeContext(List.of(role));
        ParquetForeignDataWrapper fdw = new ParquetForeignDataWrapper(new InputFactory(nodeCtx));
        BatchIterator<Row> it = fdw.getIterator(role, server, foreignTable, txnCtx,
                List.of(e.asSymbol("car_sensor['engine_temp']")),
                Literal.BOOLEAN_TRUE).get();
        List<Object[]> rows = it.toList(Row::materialize).get(10, TimeUnit.SECONDS);
        assertThat(rows).containsExactly(
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50}
        );
    }

    @Test
    public void test_projects_nested_field_from_object_type_ignored_policy() throws Exception {
        Role role = Role.CRATE_USER;
        NodeContext nodeCtx = createNodeContext(List.of(role));
        ParquetForeignDataWrapper fdw = new ParquetForeignDataWrapper(new InputFactory(nodeCtx));
        BatchIterator<Row> it = fdw.getIterator(role, server, foreignTable, txnCtx,
                List.of(e.asSymbol("car_sensor_json_ignored_policy['engine_temp']")),
                Literal.BOOLEAN_TRUE).get();
        List<Object[]> rows = it.toList(Row::materialize).get(10, TimeUnit.SECONDS);
        assertThat(rows).containsExactly(
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50}
        );
    }

    @Test
    public void test_projects_nested_field_from_object_type_dynamic_policy() throws Exception {
        e.getSessionSettings().setErrorOnUnknownObjectKey(false);
        Role role = Role.CRATE_USER;
        NodeContext nodeCtx = createNodeContext(List.of(role));
        ParquetForeignDataWrapper fdw = new ParquetForeignDataWrapper(new InputFactory(nodeCtx));
        BatchIterator<Row> it = fdw.getIterator(role, server, foreignTable, txnCtx,
                List.of(e.asSymbol("car_sensor_json['engine_temp']")),
                Literal.BOOLEAN_TRUE).get();
        List<Object[]> rows = it.toList(Row::materialize).get(10, TimeUnit.SECONDS);
        assertThat(rows).containsExactly(
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50},
            new Object[]{50}
        );
    }


    @Test
    public void test_read_from_local_is_only_allowed_for_superusers() throws Exception {
        Role role = RolesHelper.userOf("typical_user");
        NodeContext nodeCtx = createNodeContext(List.of(role));
        ParquetForeignDataWrapper fdw = new ParquetForeignDataWrapper(new InputFactory(nodeCtx));
        assertThatThrownBy(() -> {
            fdw.getIterator(role, server, foreignTable, txnCtx, List.of(e.asSymbol("passenger_count")), Literal.BOOLEAN_TRUE);
        }).isInstanceOf(UnauthorizedException.class);
    }
}
