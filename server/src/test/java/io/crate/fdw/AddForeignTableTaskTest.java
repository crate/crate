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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.crate.exceptions.RelationAlreadyExists;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class AddForeignTableTaskTest extends CrateDummyClusterServiceUnitTest {


    @Test
    public void test_cannot_create_table_if_server_is_missing() throws Exception {
        CreateForeignTableRequest request = new CreateForeignTableRequest(
            new RelationName("doc", "brown"),
            false,
            List.of(),
            "myserver",
            Map.of()
        );
        var addForeignTableTask = new AddForeignTableTask(request);
        assertThatThrownBy(() -> addForeignTableTask.execute(clusterService.state()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot create foreign table for server `myserver`. It doesn't exist. Create it using CREATE SERVER");
    }

    @Test
    public void test_cannot_create_foreign_table_if_name_is_in_use_and_if_exists() throws Exception {
        SQLExecutor.builder(clusterService)
            .addTable("create table doc.t1 (x int)")
            .addView(new RelationName("doc", "v1"), "select * from t1")
            .build();

        var addT1Conflict = new AddForeignTableTask(new CreateForeignTableRequest(
            new RelationName("doc", "t1"),
            false,
            List.of(),
            "myserver",
            Map.of()
        ));
        assertThatThrownBy(() -> addT1Conflict.execute(clusterService.state()))
            .isExactlyInstanceOf(RelationAlreadyExists.class)
            .hasMessage("Relation 'doc.t1' already exists.");


        var addV1Conflict = new AddForeignTableTask(new CreateForeignTableRequest(
            new RelationName("doc", "v1"),
            false,
            List.of(),
            "myserver",
            Map.of()
        ));
        assertThatThrownBy(() -> addV1Conflict.execute(clusterService.state()))
            .isExactlyInstanceOf(RelationAlreadyExists.class)
            .hasMessage("Relation 'doc.v1' already exists.");

        var addIfNotExists = new AddForeignTableTask(new CreateForeignTableRequest(
            new RelationName("doc", "v1"),
            true,
            List.of(),
            "myserver",
            Map.of()
        ));
        var state = clusterService.state();
        assertThat(addIfNotExists.execute(state)).isSameAs(state);
    }
}

