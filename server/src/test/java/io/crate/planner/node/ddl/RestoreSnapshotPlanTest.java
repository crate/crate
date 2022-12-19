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

package io.crate.planner.node.ddl;

import static io.crate.metadata.PartitionName.templateName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.junit.Test;

import io.crate.analyze.BoundRestoreSnapshot;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;

public class RestoreSnapshotPlanTest {

    @Test
    public void testResolveTableIndexWithIgnoreUnavailable() throws Exception {
        var context = RestoreSnapshotPlan.resolveIndexNames(
            "my_repo",
            Set.of(new BoundRestoreSnapshot.RestoreTableInfo(
                RelationName.of(Schemas.DOC_SCHEMA_NAME, "my_table"), null)),
            true,
            null
        ).get();

        assertThat(
            context.resolvedIndices(),
            containsInAnyOrder(
                "my_table",
                templateName(Schemas.DOC_SCHEMA_NAME, "my_table") + "*"));
        assertThat(context.resolvedTemplates(), contains(".partitioned.my_table."));
    }

    @Test
    public void testResolveTableIndexFromSnapshot() {
        var context = new RestoreSnapshotPlan.ResolveIndicesAndTemplatesContext();
        RestoreSnapshotPlan.resolveTableFromSnapshots(
            new BoundRestoreSnapshot.RestoreTableInfo(RelationName.of("custom", "restoreme"), null),
            List.of(
                new SnapshotInfo(
                    new SnapshotId("snapshot01", UUID.randomUUID().toString()),
                    List.of("custom.restoreme"),
                    0L,
                    false)),
            context
        );

        assertThat(context.resolvedIndices(), contains("custom.restoreme"));
        assertThat(context.resolvedTemplates().size(), is(0));
    }

    @Test
    public void testResolvePartitionedTableIndexFromSnapshot() {
        var context = new RestoreSnapshotPlan.ResolveIndicesAndTemplatesContext();
        RestoreSnapshotPlan.resolveTableFromSnapshots(
            new BoundRestoreSnapshot.RestoreTableInfo(RelationName.of(Schemas.DOC_SCHEMA_NAME, "restoreme"), null),
            List.of(new SnapshotInfo(
                new SnapshotId("snapshot01", UUID.randomUUID().toString()),
                List.of(".partitioned.restoreme.046jcchm6krj4e1g60o30c0"), 0L, false)),
            context
        );

        String template = templateName(Schemas.DOC_SCHEMA_NAME, "restoreme");
        assertThat(context.resolvedIndices(), contains(template + "*"));
        assertThat(context.resolvedTemplates(), contains(template));
    }

    @Test
    public void testResolveEmptyPartitionedTemplate() {
        var context = new RestoreSnapshotPlan.ResolveIndicesAndTemplatesContext();
        RestoreSnapshotPlan.resolveTableFromSnapshots(
            new BoundRestoreSnapshot.RestoreTableInfo(RelationName.of(Schemas.DOC_SCHEMA_NAME, "restoreme"), null),
            List.of(new SnapshotInfo(new SnapshotId("snapshot01", UUID.randomUUID().toString()), List.of(), 0L, false)),
            context
        );
        assertThat(context.resolvedIndices().size(), is(0));
        // If the snapshot doesn't contain any index which belongs to the table, it could be that the user
        // restores an empty partitioned table. For that case we attempt to restore the table template.
        assertThat(context.resolvedTemplates(), contains(templateName(Schemas.DOC_SCHEMA_NAME, "restoreme")));
    }

    @Test
    public void testResolveMultiTablesIndexNamesFromSnapshot() {
        List<BoundRestoreSnapshot.RestoreTableInfo> tables = List.of(
            new BoundRestoreSnapshot.RestoreTableInfo(
                RelationName.of(Schemas.DOC_SCHEMA_NAME, "my_table"), null),
            new BoundRestoreSnapshot.RestoreTableInfo(
                RelationName.of(Schemas.DOC_SCHEMA_NAME, "my_partitioned_table"), null));

        List<SnapshotInfo> snapshots = List.of(
            new SnapshotInfo(
                new SnapshotId("snapshot01", UUID.randomUUID().toString()),
                List.of(".partitioned.my_partitioned_table.046jcchm6krj4e1g60o30c0"),
                0,
                false),
            new SnapshotInfo(
                new SnapshotId("snapshot03", UUID.randomUUID().toString()),
                List.of("my_table"),
                0,
                false));

        var context = new RestoreSnapshotPlan.ResolveIndicesAndTemplatesContext();
        RestoreSnapshotPlan.resolveTablesFromSnapshots(tables, snapshots, context);

        assertThat(
            context.resolvedIndices(),
            containsInAnyOrder(
                "my_table",
                templateName(Schemas.DOC_SCHEMA_NAME, "my_partitioned_table") +
                "*"));
        assertThat(context.resolvedTemplates(),
                   contains(templateName(Schemas.DOC_SCHEMA_NAME, "my_partitioned_table")));
    }
}
