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

import static io.crate.testing.Asserts.assertThrowsMatches;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.junit.Before;
import org.junit.Test;

import io.crate.exceptions.InvalidRelationName;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class AlterTableRenameAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testRenamePartitionThrowsException() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Renaming a single partition is not supported");
        e.analyze("alter table t1 partition (i=1) rename to t2");
    }

    @Test
    public void testRenameToUsingSchemaThrowsException() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Target table name must not include a schema");
        e.analyze("alter table t1 rename to my_schema.t1");
    }

    @Test
    public void testRenameToInvalidName() throws Exception {
        expectedException.expect(InvalidRelationName.class);
        e.analyze("alter table t1 rename to \"foo.bar\"");
    }

    @Test
    public void test_rename_is_not_allowed_when_table_is_published() throws Exception {
        var clusterService = clusterServiceWithPublicationMetadata(false, new RelationName("doc", "t1"));
        var executor = SQLExecutor.builder(clusterService).enableDefaultTables().build();
        assertThrowsMatches(
            () ->  executor.analyze("ALTER TABLE t1 rename to t1_renamed"),
            OperationOnInaccessibleRelationException.class,
            "The relation \"doc.t1\" doesn't allow ALTER RENAME operations, because it is included in a logical replication publication."
        );
        clusterService.close();
    }

    @Test
    public void test_rename_is_not_allowed_when_all_tables_are_published() throws Exception {
        var clusterService = clusterServiceWithPublicationMetadata(true);
        var executor = SQLExecutor.builder(clusterService).enableDefaultTables().build();
        assertThrowsMatches(
            () ->  executor.analyze("ALTER TABLE t1 rename to t1_renamed"),
            OperationOnInaccessibleRelationException.class,
            "The relation \"doc.t1\" doesn't allow ALTER RENAME operations, because it is included in a logical replication publication."
        );
        clusterService.close();
    }

    private ClusterService clusterServiceWithPublicationMetadata(boolean allTablesPublished, RelationName... tables) {
        var publications = Map.of("pub1", new Publication("user1", allTablesPublished, Arrays.asList(tables)));
        var publicationsMetadata = new PublicationsMetadata(publications);
        var metadata = new Metadata.Builder().putCustom(PublicationsMetadata.TYPE, publicationsMetadata).build();
        return createClusterService(additionalClusterSettings(), metadata, Version.CURRENT);
    }
}
