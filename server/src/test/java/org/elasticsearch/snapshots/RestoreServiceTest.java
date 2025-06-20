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

package org.elasticsearch.snapshots;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.snapshots.RestoreService.resolveIndices;
import static org.elasticsearch.test.IntegTestCase.resolveIndex;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TableOrPartition;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class RestoreServiceTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void resolve_indices_multiple_tables_specified() throws Exception {
        SQLExecutor.builder(clusterService).build()
            .addTable("CREATE TABLE my_schema.table1 (id INT, name STRING)")
            .addTable("CREATE TABLE my_schema.table2 (id INT, name STRING)");

        Metadata metadata = clusterService.state().metadata();
        String indexUUID1 = resolveIndex("my_schema.table1", null, metadata).getUUID();
        String indexUUID2 = resolveIndex("my_schema.table2", null, metadata).getUUID();

        List<String> resolvedIndices = new ArrayList<>();
        List<TableOrPartition> tablesToRestore = List.of(
            new TableOrPartition(
                new RelationName("my_schema", "table1"), null
            ),
            new TableOrPartition(
                new RelationName("my_schema", "table2"), null
            )
        );

        resolveIndices(
            tablesToRestore,
            metadata,
            resolvedIndices
        );

        assertThat(resolvedIndices).contains(
            indexUUID1,
            indexUUID2
        );
    }

    /*
    @Test
    public void test_resolve_index_with_ignore_unavailable() throws Exception {
        SQLExecutor.builder(clusterService).build()
            .addTable("CREATE TABLE doc.my_table (id INT, name STRING) PARTITIONED BY (name)");

        var restoreRequest = new RestoreService.RestoreRequest(
            "repo1",
            "snapshot1",
            IndicesOptions.fromOptions(true, true, true, true),
            Settings.EMPTY,
            TimeValue.timeValueSeconds(30),
            true,
            true,
            Strings.EMPTY_ARRAY,
            true,
            Strings.EMPTY_ARRAY
        );
        List<String> resolvedIndices = new ArrayList<>();
        List<String> resolvedTemplates = new ArrayList<>();
        List<TableOrPartition> tablesToRestore = List.of(
            new TableOrPartition(new RelationName(Schemas.DOC_SCHEMA_NAME, "my_table"), null)
        );

        Metadata metadata = clusterService.state().metadata();
        //String indexUUID1 = resolveIndex("doc.my_table", null, metadata).getUUID();

        // ignoreUnavailable code path doesn't filter anything and doesn't use available indices
        List<String> availableIndices = null;
        resolveIndices(
            tablesToRestore,
            metadata,
            resolvedIndices
        );


        assertThat(resolvedIndices).containsExactlyInAnyOrder(
            "my_table",
            templateName(Schemas.DOC_SCHEMA_NAME, "my_table") + "*"
        );
        assertThat(resolvedTemplates).containsExactly(".partitioned.my_table.");
    }

     */

    @Test
    public void test_resolve_partitioned_table_index_from_snapshot() throws Exception {
        SQLExecutor.builder(clusterService).build()
            .addTable("CREATE TABLE doc.restoreme (id INT, name STRING) PARTITIONED BY (name)", ".partitioned.restoreme.046jcchm6krj4e1g60o30c0");

        Metadata metadata = clusterService.state().metadata();
        String indexUUID1 = resolveIndex("doc.restoreme", PartitionName.decodeIdent("046jcchm6krj4e1g60o30c0"), null, metadata).getUUID();

        new RestoreService.RestoreRequest(
            "repo1",
            "snapshot1",
            IndicesOptions.fromOptions(false, true, true, true),
            Settings.EMPTY,
            TimeValue.timeValueSeconds(30),
            true,
            true,
            Strings.EMPTY_ARRAY,
            true,
            Strings.EMPTY_ARRAY
        );
        List<String> resolvedIndices = new ArrayList<>();
        List<TableOrPartition> tablesToRestore = List.of(
            new TableOrPartition(new RelationName(Schemas.DOC_SCHEMA_NAME, "restoreme"), "046jcchm6krj4e1g60o30c0")
        );
        resolveIndices(
            tablesToRestore,
            metadata,
            resolvedIndices
        );

        assertThat(resolvedIndices).containsExactly(indexUUID1);
    }

    /*
    @Test
    public void test_resolve_empty_partitioned_template() {
        var restoreRequest = new RestoreService.RestoreRequest(
            "repo1",
            "snapshot1",
            IndicesOptions.fromOptions(false, true, true, true),
            Settings.EMPTY,
            TimeValue.timeValueSeconds(30),
            true,
            true,
            Strings.EMPTY_ARRAY,
            true,
            Strings.EMPTY_ARRAY
        );
        List<String> resolvedIndices = new ArrayList<>();
        List<String> resolvedTemplates = new ArrayList<>();
        List<TableOrPartition> tablesToRestore = List.of(
            new TableOrPartition(new RelationName(Schemas.DOC_SCHEMA_NAME, "restoreme"), null)
        );
        resolveIndices(
            tablesToRestore,
            Metadata.EMPTY_METADATA,
            resolvedIndices
        );

        assertThat(resolvedIndices).isEmpty();
        // If the snapshot doesn't contain any index which belongs to the table, it could be that the user
        // restores an empty partitioned table. For that case we attempt to restore the table template.
        assertThat(resolvedTemplates).containsExactly(
            templateName(Schemas.DOC_SCHEMA_NAME, "restoreme"));

    }

     */

    @Test
    public void test_resolve_multi_tables_index_names_from_snapshot() throws Exception {
        SQLExecutor.builder(clusterService).build()
            .addTable("CREATE TABLE doc.my_table (id INT, name STRING)")
            .addTable("CREATE TABLE doc.my_partitioned_table (id INT, name STRING) PARTITIONED BY (name)", ".partitioned.my_partitioned_table.046jcchm6krj4e1g60o30c0");

        Metadata metadata = clusterService.state().metadata();
        String indexUUID1 = resolveIndex("doc.my_table", null, metadata).getUUID();
        String indexUUID2 = resolveIndex("doc.my_partitioned_table", PartitionName.decodeIdent("046jcchm6krj4e1g60o30c0"), null, metadata).getUUID();

        List<String> resolvedIndices = new ArrayList<>();
        List<TableOrPartition> tablesToRestore = List.of(
            new TableOrPartition(new RelationName(Schemas.DOC_SCHEMA_NAME, "my_table"), null),
            new TableOrPartition(new RelationName(Schemas.DOC_SCHEMA_NAME, "my_partitioned_table"), "046jcchm6krj4e1g60o30c0")
        );
        resolveIndices(
            tablesToRestore,
            metadata,
            resolvedIndices
        );

        assertThat(resolvedIndices).containsExactlyInAnyOrder(
            indexUUID1,
            indexUUID2
        );
    }
}
