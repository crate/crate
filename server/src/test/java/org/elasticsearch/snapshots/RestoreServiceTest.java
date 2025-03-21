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
import static io.crate.metadata.PartitionName.templateName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.snapshots.RestoreService.resolveIndices;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TableOrPartition;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;

public class RestoreServiceTest {

    @Test
    public void resolve_indices_multiple_tables_specified() {
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
            new TableOrPartition(
                new RelationName("my_schema", "table1"), null
            ),
            new TableOrPartition(
                new RelationName("my_schema", "table2"), null
            )
        );
        List<String> availableIndices = List.of("my_schema.table1", "my_schema.table2");

        resolveIndices(
            restoreRequest,
            tablesToRestore,
            availableIndices,
            resolvedIndices,
            resolvedTemplates
        );

        assertThat(resolvedIndices).containsAll(availableIndices);
        // No partitioned table selected, templates must be empty
        assertThat(resolvedTemplates).isEmpty();
    }

    @Test
    public void test_resolve_index_with_ignore_unavailable() throws Exception {
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
        // ignoreUnavailable code path doesn't filter anything and doesn't use available indices
        List<String> availableIndices = null;
        resolveIndices(
            restoreRequest,
            tablesToRestore,
            availableIndices,
            resolvedIndices,
            resolvedTemplates
        );

        assertThat(resolvedIndices).containsExactlyInAnyOrder(
            "my_table",
            templateName(Schemas.DOC_SCHEMA_NAME, "my_table") + "*"
        );
        assertThat(resolvedTemplates).containsExactly(".partitioned.my_table.");
    }

    @Test
    public void test_resolve_partitioned_table_index_from_snapshot() {
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
            restoreRequest,
            tablesToRestore,
            List.of(".partitioned.restoreme.046jcchm6krj4e1g60o30c0"),
            resolvedIndices,
            resolvedTemplates
        );

        String template = templateName(Schemas.DOC_SCHEMA_NAME, "restoreme");
        assertThat(resolvedIndices).containsExactly(template + "*");
        assertThat(resolvedTemplates).containsExactly(template);
    }

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
            restoreRequest,
            tablesToRestore,
            List.of(""), // No available indices in the snapshot.
            resolvedIndices,
            resolvedTemplates
        );

        assertThat(resolvedIndices).isEmpty();
        // If the snapshot doesn't contain any index which belongs to the table, it could be that the user
        // restores an empty partitioned table. For that case we attempt to restore the table template.
        assertThat(resolvedTemplates).containsExactly(
            templateName(Schemas.DOC_SCHEMA_NAME, "restoreme"));

    }

    @Test
    public void test_resolve_multi_tables_index_names_from_snapshot() {
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
            new TableOrPartition(new RelationName(Schemas.DOC_SCHEMA_NAME, "my_table"), null),
            new TableOrPartition(new RelationName(Schemas.DOC_SCHEMA_NAME, "my_partitioned_table"), null)
        );
        resolveIndices(
            restoreRequest,
            tablesToRestore,
            List.of(".partitioned.my_partitioned_table.046jcchm6krj4e1g60o30c0", "my_table"),
            resolvedIndices,
            resolvedTemplates
        );

        assertThat(resolvedIndices).containsExactlyInAnyOrder(
            "my_table",
            templateName(Schemas.DOC_SCHEMA_NAME, "my_partitioned_table") +
                "*");
        assertThat(resolvedTemplates).containsExactly(
            templateName(Schemas.DOC_SCHEMA_NAME, "my_partitioned_table"));
    }




}
