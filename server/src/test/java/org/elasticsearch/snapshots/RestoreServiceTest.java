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
import static io.crate.testing.Asserts.assertThat;
import static org.elasticsearch.snapshots.RestoreService.resolveIndices;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.junit.Test;

import io.crate.metadata.RelationName;

public class RestoreServiceTest {

    @Test
    public void resolve_indices_multiple_tables_specified() {
        RestoreService.RestoreRequest restoreRequest = mock(RestoreService.RestoreRequest.class);
        boolean ignoreUnavailable = false;
        when(restoreRequest.indicesOptions()).thenReturn(
            IndicesOptions.fromOptions(ignoreUnavailable, true, true, true)
        );
        when(restoreRequest.includeIndices()).thenReturn(true);
        List<String> resolvedIndices = new ArrayList<>();
        List<String> resolvedTemplates = new ArrayList<>();
        List<RestoreSnapshotRequest.TableOrPartition> tablesToRestore = List.of(
            new RestoreSnapshotRequest.TableOrPartition(
                new RelationName("my_schema", "table1"), null
            ),
            new RestoreSnapshotRequest.TableOrPartition(
                new RelationName("my_schema", "table2"), null
            )
        );
        List<String> availableIndices = List.of("my_schema.table1", "my_schema.table2");

        resolveIndices(
            restoreRequest,
            tablesToRestore,
            availableIndices,
            Version.CURRENT,
            resolvedIndices,
            resolvedTemplates
        );

        assertThat(resolvedIndices).containsAll(availableIndices);
        assertThat(resolvedTemplates).containsExactly("_all");
    }
}
