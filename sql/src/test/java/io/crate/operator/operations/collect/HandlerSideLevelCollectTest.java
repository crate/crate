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

package io.crate.operator.operations.collect;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.WhereClause;
import io.crate.metadata.*;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.operator.operator.EqOperator;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.CollectNode;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.StringLiteral;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.common.logging.Loggers;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 1)
public class HandlerSideLevelCollectTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private HandlerSideDataCollectOperation operation;
    private Functions functions;

    @Before
    public void prepare() {
        Loggers.getLogger(HandlerSideDataCollectOperation.class).setLevel("TRACE");
        operation = cluster().getInstance(HandlerSideDataCollectOperation.class);
        functions = cluster().getInstance(Functions.class);
    }

    @Test
    public void testClusterLevel() throws Exception {
        Routing routing = new HandlerSideRouting(SysClusterTableInfo.IDENT);
        CollectNode collectNode = new CollectNode("clusterCollect", routing);

        Reference clusterNameRef = new Reference(SysClusterTableInfo.INFOS.get(new ColumnIdent("name")));
        collectNode.toCollect(Arrays.<Symbol>asList(clusterNameRef));
        collectNode.maxRowGranularity(RowGranularity.CLUSTER);
        Object[][] result = operation.collect(collectNode).get();
        assertThat(result.length, is(1));
        assertTrue(((BytesRef)result[0][0]).utf8ToString().startsWith("SUITE"));
    }

    @Test
    public void testDocLevel() throws Exception {
        HandlerSideRouting routing = new HandlerSideRouting(new TableIdent("information_schema", "tables"));
        CollectNode collectNode = new CollectNode("tablesCollect", routing);
        ReferenceInfo tableName = InformationSchemaInfo.TABLE_INFO_TABLES.getColumnInfo(new ColumnIdent("table_name"));
        Reference tableNameRef = new Reference(tableName);

        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME,
                ImmutableList.of(DataType.STRING, DataType.STRING)));
        Function whereClause = new Function(eqImpl.info(),
                ImmutableList.<Symbol>of(tableNameRef, new StringLiteral("shards")));

        collectNode.whereClause(new WhereClause(whereClause));
        collectNode.toCollect(Arrays.<Symbol>asList(tableNameRef));
        collectNode.maxRowGranularity(RowGranularity.DOC);
        Object[][] result = operation.collect(collectNode).get();

        assertThat(result.length, is(1));
        assertThat(result[0], is(new Object[]{new BytesRef("shards")}));
    }


}
