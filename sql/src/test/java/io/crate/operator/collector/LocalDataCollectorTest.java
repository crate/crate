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

package io.crate.operator.collector;

import com.carrotsearch.hppc.procedures.ObjectProcedure;
import io.crate.metadata.*;
import io.crate.operator.reference.sys.NodeNameExpression;
import io.crate.operator.reference.sys.NodePortExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.plan.CollectNode;
import io.crate.planner.symbol.Reference;
import org.cratedb.DataType;
import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.sql.CrateException;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.core.IsEqual.equalTo;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class LocalDataCollectorTest extends SQLTransportIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private ReferenceResolver referenceResolver;

    @Before
    public void getResolver() {
        referenceResolver = cluster().getInstance(ReferenceResolver.class);
    }


    @Test
    public void testCollectSysExpressions() {
        Routing routing = new Routing(new HashMap<String, Map<String, Integer>>(1){{
            put("bla", new HashMap<String, Integer>());
        }});

        CollectNode collectNode = new CollectNode("collect", routing);
        Reference nodeNameExpression = new Reference(NodeNameExpression.INFO_NAME);
        Reference nodeHTTPPortExpression = new Reference(NodePortExpression.INFO_PORT_HTTP);

        collectNode.symbols(nodeNameExpression, nodeHTTPPortExpression);
        collectNode.inputs(nodeNameExpression, nodeHTTPPortExpression);
        collectNode.outputs(nodeNameExpression, nodeHTTPPortExpression);

        LocalDataCollector collector = new LocalDataCollector(referenceResolver, collectNode);
        assertThat(collector.startCollect(), equalTo(true));
        assertThat(collector.processRow(), equalTo(false));
        Object[][] result = collector.finishCollect();
        assertThat(result.length, equalTo(1));
        ImmutableOpenMap<String, DiscoveryNode> nodeMap = clusterService().state().nodes().getNodes();
        final String[] nodeNames = new String[nodeMap.size()];
        nodeMap.values().forEach(new ObjectProcedure<DiscoveryNode>() {
            int i = 0;
            @Override
            public void apply(DiscoveryNode value) {
                nodeNames[i++] = value.getName();
            }
        });
        assertThat((String) result[0][0], isOneOf(nodeNames));
        assertThat((Integer) result[0][1], isOneOf(44200, 44201));
    }

    @Test
    public void testCollectNothing() {
        CollectNode collectNode = new CollectNode("nothing", new Routing());
        LocalDataCollector collector = new LocalDataCollector(referenceResolver, collectNode);
        assertThat(collector.startCollect(), equalTo(false));
        assertThat(collector.finishCollect(), equalTo(new Object[0][0]));
    }

    @Test
    public void testCollectUnknownReference() {

        expectedException.expect(CrateException.class);
        expectedException.expectMessage("Unknown Reference");

        CollectNode collectNode = new CollectNode("unknown", new Routing());
        Reference unknownReference = new Reference(
                new ReferenceInfo(
                        new ReferenceIdent(
                                new TableIdent("", ""),
                                ""
                        ),
                        RowGranularity.NODE,
                        DataType.BOOLEAN
                )
        );
        collectNode.symbols(unknownReference);
        collectNode.inputs(unknownReference);
        collectNode.outputs(unknownReference);
        LocalDataCollector collector = new LocalDataCollector(referenceResolver, collectNode);
    }

    @Test
    public void testCollectCorruptSymbols() {

        expectedException.expect(ElasticSearchIllegalStateException.class);
        expectedException.expectMessage("Output symbols must be in input symbols");

        CollectNode collectNode = new CollectNode("scrambled", new Routing());

        Reference nodeNameExpression = new Reference(NodeNameExpression.INFO_NAME);
        Reference nodeHTTPPortExpression = new Reference(NodePortExpression.INFO_PORT_HTTP);

        collectNode.symbols(nodeNameExpression, nodeHTTPPortExpression);
        collectNode.inputs(nodeHTTPPortExpression);
        collectNode.outputs(nodeHTTPPortExpression, nodeNameExpression);

        LocalDataCollector collector = new LocalDataCollector(referenceResolver, collectNode);
    }
}
