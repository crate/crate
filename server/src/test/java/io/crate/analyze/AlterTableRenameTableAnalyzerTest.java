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

package io.crate.analyze;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.junit.Test;

import io.crate.exceptions.InvalidRelationName;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;

public class AlterTableRenameTableAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testRenamePartitionThrowsException() throws Exception {
        var e = SQLExecutor.of(clusterService).addTable(T3.T1_DEFINITION);

        assertThatThrownBy(() -> e.analyze("alter table t1 partition (i=1) rename to t2"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Renaming a single partition is not supported");
    }

    @Test
    public void testRenameToUsingSchemaThrowsException() throws Exception {
        var e = SQLExecutor.of(clusterService).addTable(T3.T1_DEFINITION);

        assertThatThrownBy(() -> e.analyze("alter table t1 rename to my_schema.t1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Target table name must not include a schema");
    }

    @Test
    public void testRenameToInvalidName() throws Exception {
        var e = SQLExecutor.of(clusterService).addTable(T3.T1_DEFINITION);
        assertThatThrownBy(() -> e.analyze("alter table t1 rename to \"foo.bar\""))
            .isExactlyInstanceOf(InvalidRelationName.class)
            .hasMessageContaining("Relation name \"doc.foo.bar\" is invalid.");
    }

    @Test
    public void test_rename_is_not_allowed_when_table_is_published() throws Exception {
        var clusterService = clusterServiceWithPublicationMetadata(false, new RelationName("doc", "t1"));
        var executor = SQLExecutor.of(clusterService).addTable(T3.T1_DEFINITION);
        assertThatThrownBy(() -> executor.analyze("ALTER TABLE t1 rename to t1_renamed"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessageContaining(
                    "The relation \"doc.t1\" doesn't allow ALTER RENAME operations, because it is included in a logical replication publication.");
        clusterService.close();
    }

    @Test
    public void test_rename_is_not_allowed_when_all_tables_are_published() throws Exception {
        var clusterService = clusterServiceWithPublicationMetadata(true);
        var executor = SQLExecutor.of(clusterService).addTable(T3.T1_DEFINITION);
        assertThatThrownBy(() -> executor.analyze("ALTER TABLE t1 rename to t1_renamed"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessageContaining(
                    "The relation \"doc.t1\" doesn't allow ALTER RENAME operations, because it is included in a logical replication publication.");
        clusterService.close();
    }

    private ClusterService clusterServiceWithPublicationMetadata(boolean allTablesPublished, RelationName... tables) {
        var publications = Map.of("pub1", new Publication("user1", allTablesPublished, Arrays.asList(tables)));
        var publicationsMetadata = new PublicationsMetadata(publications);
        var metadata = new Metadata.Builder().putCustom(PublicationsMetadata.TYPE, publicationsMetadata).build();
        return createClusterService(additionalClusterSettings(), metadata, Version.CURRENT);
    }
}
