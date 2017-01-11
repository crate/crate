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

package io.crate.planner;

import io.crate.analyze.TableDefinitions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.WriterProjection;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.analyze.TableDefinitions.shardRouting;
import static org.hamcrest.Matchers.is;

public class CopyToPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService)
            .addDocTable(TableDefinitions.USER_TABLE_INFO)
            .addDocTable(
                new TestingTableInfo.Builder(new TableIdent("doc", "parted_generated"), shardRouting("parted_generated"))
                    .add("ts", DataTypes.TIMESTAMP, null)
                    .addGeneratedColumn("day", DataTypes.TIMESTAMP, "date_trunc('day', ts)", true)
                    .addPartitions(
                        new PartitionName("parted_generated", Arrays.asList(new BytesRef("1395874800000"))).asIndexName(),
                        new PartitionName("parted_generated", Arrays.asList(new BytesRef("1395961200000"))).asIndexName())
            ).build();
    }

    @Test
    public void testCopyToWithColumnsReferenceRewrite() throws Exception {
        Merge plan = e.plan("copy users (name) to directory '/tmp'");
        Collect innerPlan = (Collect) plan.subPlan();
        RoutedCollectPhase node = ((RoutedCollectPhase) innerPlan.collectPhase());
        Reference nameRef = (Reference) node.toCollect().get(0);

        assertThat(nameRef.ident().columnIdent().name(), is(DocSysColumns.DOC.name()));
        assertThat(nameRef.ident().columnIdent().path().get(0), is("name"));
    }

    @Test
    public void testCopyToWithPartitionedGeneratedColumn() throws Exception {
        // test that generated partition column is NOT exported
        Merge plan = e.plan("copy parted_generated to directory '/tmp'");
        Collect innerPlan = (Collect) plan.subPlan();
        RoutedCollectPhase node = ((RoutedCollectPhase) innerPlan.collectPhase());
        WriterProjection projection = (WriterProjection) node.projections().get(0);
        assertThat(projection.overwrites().size(), is(0));
    }
}
