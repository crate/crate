///*
// * Licensed to Crate.io GmbH ("Crate") under one or more contributor
// * license agreements.  See the NOTICE file distributed with this work for
// * additional information regarding copyright ownership.  Crate licenses
// * this file to you under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.  You may
// * obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// * License for the specific language governing permissions and limitations
// * under the License.
// *
// * However, if you have executed another commercial license agreement
// * with Crate these terms will supersede the license and you may use the
// * software solely pursuant to the terms of the relevant commercial agreement.
// */
//
//package io.crate.planner.optimizer.joinorder;
//
//import java.io.IOException;
//import java.util.List;
//import java.util.Map;
//import java.util.function.Function;
//
//import io.crate.analyze.WhereClause;
//import io.crate.analyze.relations.DocTableRelation;
//import io.crate.metadata.doc.DocTableInfo;
//import io.crate.planner.operators.Collect;
//import io.crate.planner.operators.HashJoin;
//import io.crate.planner.operators.LogicalPlan;
//import io.crate.planner.optimizer.costs.PlanStats;
//import io.crate.statistics.Stats;
//import io.crate.statistics.TableStats;
//import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
//import io.crate.testing.SQLExecutor;
//
//public class DPhypTest extends CrateDummyClusterServiceUnitTest {
//
//    public void test1() throws IOException {
//
//        SQLExecutor e = SQLExecutor.builder(clusterService)
//            .addTable("create table a (x int)")
//            .addTable("create table b (y int)")
//            .addTable("create table c (z int)")
//            .build();
//
//        DocTableInfo aDoc = e.resolveTableInfo("a");
//        DocTableInfo bDoc = e.resolveTableInfo("b");
//        DocTableInfo cDoc = e.resolveTableInfo("c");
//
//        var x = e.asSymbol("x");
//        var y = e.asSymbol("y");
//        var z = e.asSymbol("z");
//
//        var aCollect = new Collect(1, new DocTableRelation(aDoc), List.of(x), WhereClause.MATCH_ALL);
//        var bCollect = new Collect(2, new DocTableRelation(bDoc), List.of(y), WhereClause.MATCH_ALL);
//        var cCollect = new Collect(3, new DocTableRelation(cDoc), List.of(z), WhereClause.MATCH_ALL);
//
//        var hashjoinAB = new HashJoin(4, aCollect, bCollect, e.asSymbol("a.x = b.y"));
//        var hashjoinABC = new HashJoin(5, hashjoinAB, cCollect, e.asSymbol("b.y = c.z"));
//
//        Graph joinGraph = Graph.create(hashjoinABC, Function.identity());
//
//        TableStats tableStats = new TableStats();
//        tableStats.updateTableStats(
//            Map.of(
//            aDoc.ident(), new Stats(1000, 1000, Map.of()),
//            bDoc.ident(), new Stats(1000, 1000, Map.of()),
//            cDoc.ident(), new Stats(1000, 1000, Map.of())
//        ));
//        PlanStats planStats = new PlanStats(tableStats);
//        Function<LogicalPlan, Long> costFunction = (LogicalPlan p) -> planStats.get(p).numDocs();
//
//        DPhyp dPhyp = new DPhyp(joinGraph, costFunction);
//        dPhyp.solve();
//    }
//}
