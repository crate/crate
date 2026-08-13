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

package io.crate.execution.ddl;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.Test;

import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class SwapRelationsOperationTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_rejects_swap_if_source_name_resolves_to_different_table_oid() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table a (x int)")
            .addTable("create table b (x int)");
        DocTableInfo b = e.resolveTableInfo("b");

        var operation = new SwapRelationsOperation(null, null);
        var request = new SwapRelationsRequest(
            List.of(new RelationNameSwap(
                new RelationName("doc", "a"),
                b.oid(),
                new RelationName("doc", "b"),
                b.oid()
            )),
            List.of()
        );

        assertThatThrownBy(() -> operation.execute(clusterService.state(), request))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessage("Relation 'doc.a' changed while SWAP TABLE was being executed");
    }

    @Test
    public void test_rejects_swap_if_target_name_resolves_to_different_table_oid() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table a (x int)")
            .addTable("create table b (x int)");
        DocTableInfo a = e.resolveTableInfo("a");

        var operation = new SwapRelationsOperation(null, null);
        var request = new SwapRelationsRequest(
            List.of(new RelationNameSwap(
                new RelationName("doc", "a"),
                a.oid(),
                new RelationName("doc", "b"),
                a.oid()
            )),
            List.of()
        );

        assertThatThrownBy(() -> operation.execute(clusterService.state(), request))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessage("Relation 'doc.b' changed while SWAP TABLE was being executed");
    }
}
