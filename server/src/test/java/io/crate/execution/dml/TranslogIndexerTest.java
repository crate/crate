/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.dml;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;

import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class TranslogIndexerTest extends CrateDummyClusterServiceUnitTest {

    public void test_translog_error_on_mapping_update() throws IOException {
        SQLExecutor executor = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int)");
        DocTableInfo table = executor.resolveTableInfo("tbl");
        assertThat(table).isNotNull();

        TranslogIndexer ti = table.getTranslogIndexer();
        BytesReference source = new BytesArray("{\"1\":1,\"2\":2}");
        assertThatThrownBy(() -> ti.index("1", source))
            .isExactlyInstanceOf(TranslogMappingUpdateException.class)
            .hasMessageContaining("Unknown column in translog entry: 2=2");
    }
}
