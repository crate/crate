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

import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.RelationUnknown;
import io.crate.expression.symbol.Symbol;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.Table;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.contains;

public class RefreshAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .addBlobTable("create blob table blobs")
            .build();
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
        AnalyzedRefreshTable analysis = e.analyze("REFRESH TABLE parted PARTITION (date=1395874800000)");
        Set<Table<Symbol>> analyzedTables = analysis.tables().keySet();
        assertThat(analyzedTables.size(), is(1));

        List<Assignment<Symbol>> partitionProperties = analyzedTables.iterator().next().partitionProperties();
        assertThat(partitionProperties.size(), is(1));
        assertThat(partitionProperties.get(0).columnName(), isLiteral("date"));
        assertThat(partitionProperties.get(0).expressions(), contains(isLiteral(1395874800000L)));
    }

    @Test
    public void testRefreshMultipleTablesUnknown() throws Exception {
        expectedException.expect(RelationUnknown.class);
        expectedException.expectMessage("Relation 'foo' unknown");
        e.analyze("REFRESH TABLE parted, foo, bar");
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
        AnalyzedRefreshTable analysis = e.analyze("REFRESH TABLE parted PARTITION (date=null)");
        Set<Table<Symbol>> analyzedTables = analysis.tables().keySet();
        assertThat(analyzedTables.size(), is(1));

        List<Assignment<Symbol>> partitionProperties = analyzedTables.iterator().next().partitionProperties();
        assertThat(partitionProperties.size(), is(1));
        assertThat(partitionProperties.get(0).expressions(), contains(isLiteral(null)));
    }
}
