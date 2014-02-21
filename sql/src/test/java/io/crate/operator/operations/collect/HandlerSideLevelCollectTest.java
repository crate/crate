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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 1)
public class HandlerSideLevelCollectTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private HandlerSideDataCollectOperation operation;
    private Functions functions;


    private String printedTable(Object[][] result) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        for (Object[] row : result) {
            boolean first = true;
            for (Object o : row) {
                if (!first) {
                    out.print("| ");
                } else {
                    first = false;
                }
                if (o == null) {
                    out.print("NULL");
                } else if (o instanceof BytesRef) {
                    out.print(((BytesRef) o).utf8ToString());
                } else {
                    out.print(o.toString());
                }
            }
            out.println();
        }
        return os.toString();
    }

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
        assertTrue(((BytesRef) result[0][0]).utf8ToString().startsWith("SUITE"));
    }

    @Test
    public void testInformationSchemaTables() throws Exception {
        HandlerSideRouting routing = new HandlerSideRouting(new TableIdent("information_schema", "tables"));
        CollectNode collectNode = new CollectNode("tablesCollect", routing);

        List<Symbol> toCollect = new ArrayList<>();
        for (ReferenceInfo info : InformationSchemaInfo.TABLE_INFO_TABLES.columns()) {
            toCollect.add(new Reference(info));
        }
        Symbol tableNameRef = toCollect.get(1);

        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME,
                ImmutableList.of(DataType.STRING, DataType.STRING)));
        Function whereClause = new Function(eqImpl.info(),
                ImmutableList.of(tableNameRef, new StringLiteral("shards")));

        collectNode.whereClause(new WhereClause(whereClause));
        collectNode.toCollect(toCollect);
        collectNode.maxRowGranularity(RowGranularity.DOC);
        Object[][] result = operation.collect(collectNode).get();
        System.out.println(printedTable(result));
        assertEquals("sys| shards| 1| 0| NULL\n", printedTable(result));
    }


    @Test
    public void testInformationSchemaColumns() throws Exception {
        HandlerSideRouting routing = new HandlerSideRouting(new TableIdent("information_schema", "columns"));
        CollectNode collectNode = new CollectNode("columnsCollect", routing);

        List<Symbol> toCollect = new ArrayList<>();
        for (ReferenceInfo info : InformationSchemaInfo.TABLE_INFO_COLUMNS.columns()) {
            toCollect.add(new Reference(info));
        }
        collectNode.toCollect(toCollect);
        collectNode.maxRowGranularity(RowGranularity.DOC);
        Object[][] result = operation.collect(collectNode).get();


        String expected = "sys| cluster| id| 1| string\n" +
                "sys| cluster| name| 2| string\n" +
                "sys| nodes| id| 1| string\n" +
                "sys| nodes| name| 2| string";


        assertTrue(printedTable(result).startsWith(expected));

        // second time - to check if the internal iterator resets
        System.out.println(printedTable(result));
        result = operation.collect(collectNode).get();
        assertTrue(printedTable(result).startsWith(expected));
    }

}
