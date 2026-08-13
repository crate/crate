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

package io.crate.metadata.cluster;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.crate.execution.ddl.tables.RenameTableRequest;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class RenameTableClusterStateExecutorTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_rejects_rename_if_source_name_resolves_to_different_table_oid() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table a (x int)")
            .addTable("create table b (x int)");
        DocTableInfo b = e.resolveTableInfo("b");

        var executor = new RenameTableClusterStateExecutor(null, null);
        var request = new RenameTableRequest(
            new RelationName("doc", "a"),
            b.oid(),
            new RelationName("doc", "target"),
            false
        );

        assertThatThrownBy(() -> executor.execute(clusterService.state(), request))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessage("Relation 'doc.a' changed while RENAME TABLE was being executed");
    }
}
