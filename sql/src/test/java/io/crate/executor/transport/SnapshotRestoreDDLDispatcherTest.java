/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.executor.transport;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.RestoreSnapshotAnalyzedStatement;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotRestoreDDLDispatcherTest extends CrateUnitTest {

    @Test
    public void testResolveTableIndexWithIgnoreUnavailable() throws Exception {
        CompletableFuture<SnapshotRestoreDDLDispatcher.ResolveIndicesAndTemplatesContext> f = SnapshotRestoreDDLDispatcher.resolveIndexNames(
            Collections.singletonList(new RestoreSnapshotAnalyzedStatement.RestoreTableInfo(new TableIdent(Schemas.DOC_SCHEMA_NAME, "my_table"),null)),
            true, null, "my_repo"
        );
        SnapshotRestoreDDLDispatcher.ResolveIndicesAndTemplatesContext ctx = f.get();
        assertThat(ctx.resolvedIndices(), containsInAnyOrder("my_table", PartitionName.templateName(Schemas.DOC_SCHEMA_NAME, "my_table") + "*"));
        assertThat(ctx.resolvedTemplates(), contains(".partitioned.my_table."));
    }

    @Test
    public void testResolveTableIndexFromSnapshot() throws Exception {
        SnapshotRestoreDDLDispatcher.ResolveIndicesAndTemplatesContext ctx = new SnapshotRestoreDDLDispatcher.ResolveIndicesAndTemplatesContext();
        SnapshotRestoreDDLDispatcher.ResolveFromSnapshotActionListener.resolveTableFromSnapshot(
            new RestoreSnapshotAnalyzedStatement.RestoreTableInfo(new TableIdent("custom", "restoreme"), null),
            Collections.singletonList(
                new SnapshotInfo(new SnapshotId("snapshot01", UUID.randomUUID().toString()), Collections.singletonList("custom.restoreme"), 0L)
            ),
            ctx
        );
        assertThat(ctx.resolvedIndices(), contains("custom.restoreme"));
        assertThat(ctx.resolvedTemplates().size(), is(0));
    }

    @Test
    public void testResolvePartitionedTableIndexFromSnapshot() throws Exception {
        SnapshotRestoreDDLDispatcher.ResolveIndicesAndTemplatesContext ctx = new SnapshotRestoreDDLDispatcher.ResolveIndicesAndTemplatesContext();
        SnapshotRestoreDDLDispatcher.ResolveFromSnapshotActionListener.resolveTableFromSnapshot(
            new RestoreSnapshotAnalyzedStatement.RestoreTableInfo(new TableIdent(Schemas.DOC_SCHEMA_NAME, "restoreme"), null),
            Collections.singletonList(
                new SnapshotInfo(new SnapshotId("snapshot01", UUID.randomUUID().toString()),
                    Collections.singletonList(".partitioned.restoreme.046jcchm6krj4e1g60o30c0"), 0L)
            ),
            ctx
        );
        String template = PartitionName.templateName(Schemas.DOC_SCHEMA_NAME, "restoreme");
        assertThat(ctx.resolvedIndices(), contains(template + "*"));
        assertThat(ctx.resolvedTemplates(), contains(template));
    }

    @Test
    public void testResolveEmptyPartitionedTemplate() throws Exception {
        SnapshotRestoreDDLDispatcher.ResolveIndicesAndTemplatesContext ctx = new SnapshotRestoreDDLDispatcher.ResolveIndicesAndTemplatesContext();
        SnapshotRestoreDDLDispatcher.ResolveFromSnapshotActionListener.resolveTableFromSnapshot(
            new RestoreSnapshotAnalyzedStatement.RestoreTableInfo(new TableIdent(Schemas.DOC_SCHEMA_NAME, "restoreme"), null),
            Collections.singletonList(
                new SnapshotInfo(new SnapshotId("snapshot01", UUID.randomUUID().toString()), ImmutableList.of(), 0L)
            ),
            ctx
        );
        assertThat(ctx.resolvedIndices().size(), is(0));
        // If the snapshot doesn't contain any index which belongs to the table, it could be that the user
        // restores an empty partitioned table. For that case we attempt to restore the table template.
        assertThat(ctx.resolvedTemplates(), contains(PartitionName.templateName(Schemas.DOC_SCHEMA_NAME, "restoreme")));
    }

    @Test
    public void testResolveMultiTablesIndexNamesFromSnapshot() throws Exception {
        List<RestoreSnapshotAnalyzedStatement.RestoreTableInfo> tables = Arrays.asList(
            new RestoreSnapshotAnalyzedStatement.RestoreTableInfo(new TableIdent(Schemas.DOC_SCHEMA_NAME, "my_table"), null),
            new RestoreSnapshotAnalyzedStatement.RestoreTableInfo(new TableIdent(Schemas.DOC_SCHEMA_NAME, "my_partitioned_table"), null)
        );
        List<SnapshotInfo> snapshots = Arrays.asList(
                new SnapshotInfo(
                    new SnapshotId("snapshot01", UUID.randomUUID().toString()), Collections.singletonList(".partitioned.my_partitioned_table.046jcchm6krj4e1g60o30c0"), 0),
                new SnapshotInfo(new SnapshotId("snapshot03", UUID.randomUUID().toString()), Collections.singletonList("my_table"), 0)
            );

        CompletableFuture<SnapshotRestoreDDLDispatcher.ResolveIndicesAndTemplatesContext> future = new CompletableFuture<>();
        SnapshotRestoreDDLDispatcher.ResolveFromSnapshotActionListener actionListener =
            new SnapshotRestoreDDLDispatcher.ResolveFromSnapshotActionListener(future, tables, new SnapshotRestoreDDLDispatcher.ResolveIndicesAndTemplatesContext());

        // need to mock here as constructor is not accessible
        GetSnapshotsResponse response = mock(GetSnapshotsResponse.class);
        when(response.getSnapshots()).thenReturn(snapshots);
        actionListener.onResponse(response);

        SnapshotRestoreDDLDispatcher.ResolveIndicesAndTemplatesContext ctx = future.get();
        assertThat(ctx.resolvedIndices(), containsInAnyOrder("my_table", PartitionName.templateName(Schemas.DOC_SCHEMA_NAME, "my_partitioned_table") + "*"));
        assertThat(ctx.resolvedTemplates(), contains(PartitionName.templateName(Schemas.DOC_SCHEMA_NAME, "my_partitioned_table")));
    }

}
