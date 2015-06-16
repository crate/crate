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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.UpdateAnalyzedStatement;
import io.crate.analyze.VersionRewriter;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.where.DocKeys;
import io.crate.metadata.PartitionName;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.planner.*;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dml.SymbolBasedUpsertByIdNode;
import io.crate.planner.node.dml.Upsert;
import io.crate.planner.node.dql.CollectAndMerge;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.UpdateProjection;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.ValueSymbolVisitor;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.routing.operation.plain.Preference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Singleton
public class UpdateConsumer implements Consumer {

    private final Visitor visitor;

    @Inject
    public UpdateConsumer() {
        visitor = new Visitor();
    }

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        return visitor.process(rootRelation, context);
    }

    class Visitor extends AnalyzedRelationVisitor<ConsumerContext, PlannedAnalyzedRelation> {

        @Override
        public PlannedAnalyzedRelation visitUpdateAnalyzedStatement(UpdateAnalyzedStatement statement, ConsumerContext context) {
            assert statement.sourceRelation() instanceof TableRelation : "sourceRelation of update statement must be a TableRelation";
            TableRelation tableRelation = (TableRelation) statement.sourceRelation();
            TableInfo tableInfo = tableRelation.tableInfo();

            if (tableInfo.schemaInfo().systemSchema() || tableInfo.rowGranularity() != RowGranularity.DOC) {
                return null;
            }

            List<Plan> childNodes = new ArrayList<>(statement.nestedStatements().size());
            SymbolBasedUpsertByIdNode upsertByIdNode = null;
            for (UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalysis : statement.nestedStatements()) {
                WhereClause whereClause = nestedAnalysis.whereClause();
                if (whereClause.noMatch()){
                    continue;
                }
                if (whereClause.docKeys().isPresent()) {
                    if (upsertByIdNode == null) {
                        Tuple<String[], Symbol[]> assignments = convertAssignments(nestedAnalysis.assignments());
                        upsertByIdNode = new SymbolBasedUpsertByIdNode(context.plannerContext().nextExecutionNodeId(), false, statement.nestedStatements().size() > 1, assignments.v1(), null);
                        childNodes.add(new IterablePlan(upsertByIdNode));
                    }
                    upsertById(nestedAnalysis, tableInfo, whereClause, upsertByIdNode);
                } else {
                    Plan plan = upsertByQuery(nestedAnalysis, context, tableInfo, whereClause);
                    if (plan != null) {
                        childNodes.add(plan);
                    }
                }
            }
            if (childNodes.size() > 0){
                return new Upsert(childNodes);
            } else {
                return new NoopPlannedAnalyzedRelation(statement);
            }
        }

        private Plan upsertByQuery(UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalysis,
                                   ConsumerContext consumerContext,
                                   TableInfo tableInfo,
                                   WhereClause whereClause) {

            Symbol versionSymbol = null;
            if(whereClause.hasVersions()){
                versionSymbol = VersionRewriter.get(whereClause.query());
                whereClause = new WhereClause(whereClause.query(), whereClause.docKeys().orNull(), whereClause.partitions());
            }


            if (!whereClause.noMatch() || !(tableInfo.isPartitioned() && whereClause.partitions().isEmpty())) {
                // for updates, we always need to collect the `_uid`
                Reference uidReference = new Reference(
                        new ReferenceInfo(
                                new ReferenceIdent(tableInfo.ident(), "_uid"),
                                RowGranularity.DOC, DataTypes.STRING));

                Tuple<String[], Symbol[]> assignments = convertAssignments(nestedAnalysis.assignments());

                Long version = null;
                if (versionSymbol != null){
                    version = ValueSymbolVisitor.LONG.process(versionSymbol);
                }

                UpdateProjection updateProjection = new UpdateProjection(
                        new InputColumn(0, DataTypes.STRING),
                        assignments.v1(),
                        assignments.v2(),
                        version);

                CollectNode collectNode = PlanNodeBuilder.collect(
                        tableInfo,
                        consumerContext.plannerContext(),
                        whereClause,
                        ImmutableList.<Symbol>of(uidReference),
                        ImmutableList.<Projection>of(updateProjection),
                        null,
                        Preference.PRIMARY.type()
                );
                MergeNode mergeNode = PlanNodeBuilder.localMerge(
                        ImmutableList.<Projection>of(CountAggregation.PARTIAL_COUNT_AGGREGATION_PROJECTION), collectNode,
                        consumerContext.plannerContext());
                return new CollectAndMerge(collectNode, mergeNode);
            } else {
                return null;
            }
        }

        private void upsertById(UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalysis,
                                             TableInfo tableInfo,
                                             WhereClause whereClause,
                                             SymbolBasedUpsertByIdNode upsertByIdNode) {
            String[] indices = Planner.indices(tableInfo, whereClause);
            assert tableInfo.isPartitioned() || indices.length == 1;

            Tuple<String[], Symbol[]> assignments = convertAssignments(nestedAnalysis.assignments());


            for (DocKeys.DocKey key : whereClause.docKeys().get()) {
                String index;
                if (key.partitionValues().isPresent()) {
                    index = new PartitionName(tableInfo.ident(), key.partitionValues().get()).stringValue();
                } else {
                    index = indices[0];
                }
                upsertByIdNode.add(
                        index,
                        key.id(),
                        key.routing(),
                        assignments.v2(),
                        key.version().orNull());
            }
        }


        private Tuple<String[], Symbol[]> convertAssignments(Map<Reference, Symbol> assignments) {
            String[] assignmentColumns = new String[assignments.size()];
            Symbol[] assignmentSymbols = new Symbol[assignments.size()];
            Iterator<Reference> it = assignments.keySet().iterator();
            int i = 0;
            while(it.hasNext()) {
                Reference key = it.next();
                assignmentColumns[i] = key.ident().columnIdent().fqn();
                assignmentSymbols[i] = assignments.get(key);
                i++;
            }
            return new Tuple<>(assignmentColumns, assignmentSymbols);
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }
    }
}
