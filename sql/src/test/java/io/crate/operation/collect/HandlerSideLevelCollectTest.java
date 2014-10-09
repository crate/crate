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

package io.crate.operation.collect;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.where.WhereClause;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.*;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.MapBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class HandlerSideLevelCollectTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private HandlerSideDataCollectOperation operation;
    private Functions functions;


    @Before
    public void prepare() {
        operation = cluster().getInstance(HandlerSideDataCollectOperation.class);
        functions = cluster().getInstance(Functions.class);
    }

    @Test
    public void testClusterLevel() throws Exception {
        Routing routing = SysClusterTableInfo.ROUTING;
        CollectNode collectNode = new CollectNode("clusterCollect", routing);

        Reference clusterNameRef = new Reference(SysClusterTableInfo.INFOS.get(new ColumnIdent("name")));
        collectNode.toCollect(Arrays.<Symbol>asList(clusterNameRef));
        collectNode.maxRowGranularity(RowGranularity.CLUSTER);
        Object[][] result = operation.collect(collectNode).get();
        assertThat(result.length, is(1));
        assertTrue(((BytesRef) result[0][0]).utf8ToString().startsWith("shared-"));
    }

    @Test
    public void testInformationSchemaTables() throws Exception {
        Routing routing = new Routing(MapBuilder.<String, Map<String, Set<Integer>>>newMapBuilder().put(
                null, MapBuilder.<String, Set<Integer>>newMapBuilder().put("information_schema.tables", null).map()
        ).map());
        CollectNode collectNode = new CollectNode("tablesCollect", routing);

        InformationSchemaInfo schemaInfo =  cluster().getInstance(InformationSchemaInfo.class);
        TableInfo tablesTableInfo = schemaInfo.getTableInfo("tables");
        List<Symbol> toCollect = new ArrayList<>();
        for (ReferenceInfo info : tablesTableInfo.columns()) {
            toCollect.add(new Reference(info));
        }
        Symbol tableNameRef = toCollect.get(1);

        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME,
                ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.STRING)));
        Function whereClause = new Function(eqImpl.info(),
                ImmutableList.of(tableNameRef, Literal.newLiteral("shards")));

        collectNode.whereClause(new WhereClause(whereClause));
        collectNode.toCollect(toCollect);
        collectNode.maxRowGranularity(RowGranularity.DOC);
        Object[][] result = operation.collect(collectNode).get();
        System.out.println(TestingHelpers.printedTable(result));
        assertEquals("sys| shards| 1| 0| NULL| NULL| NULL\n", TestingHelpers.printedTable(result));
    }


    @Test
    public void testInformationSchemaColumns() throws Exception {
        Routing routing = new Routing(MapBuilder.<String, Map<String, Set<Integer>>>newMapBuilder().put(
                null, MapBuilder.<String, Set<Integer>>newMapBuilder().put("information_schema.columns", null).map()
        ).map());
        CollectNode collectNode = new CollectNode("columnsCollect", routing);

        InformationSchemaInfo schemaInfo =  cluster().getInstance(InformationSchemaInfo.class);
        TableInfo tableInfo = schemaInfo.getTableInfo("columns");
        List<Symbol> toCollect = new ArrayList<>();
        for (ReferenceInfo info : tableInfo.columns()) {
            toCollect.add(new Reference(info));
        }
        collectNode.toCollect(toCollect);
        collectNode.maxRowGranularity(RowGranularity.DOC);
        Object[][] result = operation.collect(collectNode).get();


        String expected = "sys| cluster| id| 1| string\n" +
                "sys| cluster| name| 2| string\n" +
                "sys| cluster| master_node| 3| string\n" +
                "sys| cluster| settings| 4| object";


        assertTrue(TestingHelpers.printedTable(result).startsWith(expected));

        // second time - to check if the internal iterator resets
        System.out.println(TestingHelpers.printedTable(result));
        result = operation.collect(collectNode).get();
        assertTrue(TestingHelpers.printedTable(result).startsWith(expected));
    }

}
