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

package io.crate.planner.statement;

import io.crate.analyze.DeleteAnalyzedStatement;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.where.DocKeys;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.IterablePlan;
import io.crate.planner.NoopPlan;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.node.ddl.ESDeletePartitionNode;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.planner.node.dml.ESDeleteNode;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.List;

@Singleton
public class DeleteStatementPlanner {

    public Plan planDelete(DeleteAnalyzedStatement analyzedStatement, Planner.Context context) {
        IterablePlan plan = new IterablePlan(context.jobId());
        DocTableRelation tableRelation = analyzedStatement.analyzedRelation();
        List<WhereClause> whereClauses = new ArrayList<>(analyzedStatement.whereClauses().size());
        List<DocKeys.DocKey> docKeys = new ArrayList<>(analyzedStatement.whereClauses().size());
        for (WhereClause whereClause : analyzedStatement.whereClauses()) {
            if (whereClause.noMatch()) {
                continue;
            }
            if (whereClause.docKeys().isPresent() && whereClause.docKeys().get().size() == 1) {
                docKeys.add(whereClause.docKeys().get().getOnlyKey());
            } else if (!whereClause.noMatch()) {
                whereClauses.add(whereClause);
            }
        }
        if (!docKeys.isEmpty()) {
            plan.add(new ESDeleteNode(context.nextExecutionPhaseId(), tableRelation.tableInfo(), docKeys));
        } else if (!whereClauses.isEmpty()) {
            createESDeleteByQueryNode(tableRelation.tableInfo(), whereClauses, plan, context);
        }

        if (plan.isEmpty()) {
            return new NoopPlan(context.jobId());
        }
        return plan;
    }

    private void createESDeleteByQueryNode(DocTableInfo tableInfo,
                                           List<WhereClause> whereClauses,
                                           IterablePlan plan,
                                           Planner.Context context) {

        List<String[]> indicesList = new ArrayList<>(whereClauses.size());
        for (WhereClause whereClause : whereClauses) {
            String[] indices = Planner.indices(tableInfo, whereClause);
            if (indices.length > 0) {
                if (!whereClause.hasQuery() && tableInfo.isPartitioned()) {
                    plan.add(new ESDeletePartitionNode(indices));
                } else {
                    indicesList.add(indices);
                }
            }
        }
        // TODO: if we allow queries like 'partitionColumn=X or column=Y' which is currently
        // forbidden through analysis, we must issue deleteByQuery request in addition
        // to above deleteIndex request(s)
        if (!indicesList.isEmpty()) {
            plan.add(new ESDeleteByQueryNode(context.nextExecutionPhaseId(), indicesList, whereClauses));
        }
    }

}
