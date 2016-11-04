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

import io.crate.analyze.RestoreSnapshotAnalyzedStatement;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotRestoreDDLDispatcherTest extends CrateUnitTest {

    @Test
    public void testResolveTableIndexWithIgnoreUnavailable() throws Exception {
        CompletableFuture<List<String>> f = SnapshotRestoreDDLDispatcher.resolveIndexNames(
            Collections.singletonList(new RestoreSnapshotAnalyzedStatement.RestoreTableInfo(new TableIdent(null, "my_table"),null)),
            true, null, "my_repo"
        );
        assertThat(f.get(), containsInAnyOrder("my_table", PartitionName.templateName(null, "my_table") + "*"));
    }

    @Test
    public void testResolveTableIndexFromSnapshot() throws Exception {
        String resolvedIndex = SnapshotRestoreDDLDispatcher.ResolveFromSnapshotActionListener.resolveIndexNameFromSnapshot(
            new TableIdent("custom", "restoreme"),
            Collections.singletonList(
                new SnapshotInfo(new SnapshotId("snapshot01", UUID.randomUUID().toString()), Collections.singletonList("custom.restoreme"), 0L)
            )
        );
        assertThat(resolvedIndex, is("custom.restoreme"));
    }

    @Test
    public void testResolvePartitionedTableIndexFromSnapshot() throws Exception {
        String resolvedIndex = SnapshotRestoreDDLDispatcher.ResolveFromSnapshotActionListener.resolveIndexNameFromSnapshot(
            new TableIdent(null, "restoreme"),
            Collections.singletonList(
                new SnapshotInfo(new SnapshotId("snapshot01", UUID.randomUUID().toString()),
                    Collections.singletonList(".partitioned.restoreme.046jcchm6krj4e1g60o30c0"), 0L)
            )
        );
        String template = PartitionName.templateName(null, "restoreme") + "*";
        assertThat(resolvedIndex, is(template));
    }

    @Test
    public void testResolveMultiTablesIndexNamesFromSnapshot() throws Exception {
        List<RestoreSnapshotAnalyzedStatement.RestoreTableInfo> tables = Arrays.asList(
            new RestoreSnapshotAnalyzedStatement.RestoreTableInfo(new TableIdent(null, "my_table"), null),
            new RestoreSnapshotAnalyzedStatement.RestoreTableInfo(new TableIdent(null, "my_partitioned_table"), null)
        );
        List<SnapshotInfo> snapshots = Arrays.asList(
                new SnapshotInfo(
                    new SnapshotId("snapshot01", UUID.randomUUID().toString()), Collections.singletonList(".partitioned.my_partitioned_table.046jcchm6krj4e1g60o30c0"), 0),
                new SnapshotInfo(new SnapshotId("snapshot03", UUID.randomUUID().toString()), Collections.singletonList("my_table"), 0)
            );

        CompletableFuture<List<String>> future = new CompletableFuture<>();
        SnapshotRestoreDDLDispatcher.ResolveFromSnapshotActionListener actionListener =
            new SnapshotRestoreDDLDispatcher.ResolveFromSnapshotActionListener(future, tables, new HashSet<>());

        // need to mock here as constructor is not accessible
        GetSnapshotsResponse response = mock(GetSnapshotsResponse.class);
        when(response.getSnapshots()).thenReturn(snapshots);
        actionListener.onResponse(response);
        assertThat(future.get(), containsInAnyOrder("my_table", PartitionName.templateName(null, "my_partitioned_table") + "*"));
    }

    @Test
    public void testResolveUnknownTableFromSnapshot() throws Exception {
        expectedException.expect(TableUnknownException.class);
        expectedException.expectMessage("Table 'doc.t1' unknown");
        SnapshotRestoreDDLDispatcher.ResolveFromSnapshotActionListener.resolveIndexNameFromSnapshot(new TableIdent(null, "t1"), Collections.emptyList());
    }

}
