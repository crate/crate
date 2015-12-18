/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.consumer;


import com.google.common.base.Optional;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.NoopPlan;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.ESGetNode;

import java.util.UUID;

public class ESGetStatementPlanner {


    public static Plan convert(QueriedDocTable table, Planner.Context context) {
        assert !(table.querySpec().hasAggregates()
                || table.querySpec().groupBy().isPresent()
                || !table.querySpec().where().docKeys().isPresent());

        DocTableInfo tableInfo = table.tableRelation().tableInfo();
        if(table.querySpec().where().docKeys().get().withVersions()){
            throw new VersionInvalidException();
        }
        Optional<Integer> limit = table.querySpec().limit();
        if (limit.isPresent() && limit.get() == 0){
            return new NoopPlan(context.jobId());
        }
        table.tableRelation().validateOrderBy(table.querySpec().orderBy());
        return new ESGetNode(context.nextExecutionPhaseId(), tableInfo, table.querySpec(), context.jobId()).plan();
    }
}
