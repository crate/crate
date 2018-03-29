/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.apache.lucene.util.BytesRef;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class RefreshAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private RelationName myBlobsIdent = new RelationName(BlobSchemaInfo.NAME, "blobs");
    private SQLExecutor e;

    @Before
    public void prepare() {
        TestingBlobTableInfo myBlobsTableInfo = TableDefinitions.createBlobTable(myBlobsIdent);
        e = SQLExecutor.builder(clusterService).enableDefaultTables().addBlobTable(myBlobsTableInfo).build();
    }

    @Test
    public void testRefreshSystemTable() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"sys.shards\" doesn't support or allow REFRESH " +
                                        "operations, as it is read-only.");
        e.analyze("refresh table sys.shards");
    }

    @Test
    public void testRefreshBlobTable() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"blob.blobs\" doesn't support or allow REFRESH " +
                                        "operations.");
        e.analyze("refresh table blob.blobs");
    }

    @Test
    public void testRefreshPartition() throws Exception {
        PartitionName partition = new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000")));
        RefreshTableAnalyzedStatement analysis = e.analyze("refresh table parted PARTITION (date=1395874800000)");
        assertThat(analysis.indexNames(), contains(".partitioned.parted.04732cpp6ks3ed1o60o30c1g"));
    }

    @Test(expected = RelationUnknown.class)
    public void testRefreshMultipleTablesUnknown() throws Exception {
        RefreshTableAnalyzedStatement analysis = e.analyze("refresh table parted, foo, bar");

        assertThat(analysis.indexNames().size(), is(1));
        assertThat(analysis.indexNames(), contains(Matchers.hasToString("doc.parted")));
    }

    @Test
    public void testRefreshInvalidPartitioned() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        e.analyze("refresh table parted partition (invalid_column='hddsGNJHSGFEFZÜ')");
    }

    @Test
    public void testRefreshNonPartitioned() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        e.analyze("refresh table users partition (foo='n')");
    }

    @Test
    public void testRefreshSysPartitioned() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"sys.shards\" doesn't support or allow REFRESH" +
                                        " operations, as it is read-only.");
        e.analyze("refresh table sys.shards partition (id='n')");
    }

    @Test
    public void testRefreshBlobPartitioned() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"blob.blobs\" doesn't support or allow REFRESH " +
                                        "operations.");
        e.analyze("refresh table blob.blobs partition (n='n')");
    }

    @Test
    public void testRefreshPartitionedTableNullPartition() throws Exception {
        RefreshTableAnalyzedStatement analysis = e.analyze("refresh table parted PARTITION (date=null)");
        assertThat(analysis.indexNames(), contains(Matchers.hasToString(".partitioned.parted.0400")));
    }
}
