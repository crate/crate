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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocReferenceConverter;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.ScoreReferenceDetector;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.MergeProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QueryThenFetchConsumer implements Consumer {

    private static final Visitor VISITOR = new Visitor();
    private static final ScoreReferenceDetector SCORE_REFERENCE_DETECTOR = new ScoreReferenceDetector();
    private static final ColumnIdent DOC_ID_COLUMN_IDENT = new ColumnIdent(DocSysColumns.DOCID.name());
    private static final InputColumn DEFAULT_DOC_ID_INPUT_COLUMN = new InputColumn(0, DataTypes.STRING);
    private static final InputColumn DEFAULT_SCORE_INPUT_COLUMN = new InputColumn(1, DataTypes.FLOAT);

    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        PlannedAnalyzedRelation plannedAnalyzedRelation = VISITOR.process(rootRelation, context);
        if (plannedAnalyzedRelation == null) {
            return false;
        }
        context.rootRelation(plannedAnalyzedRelation);
        return true;
    }

    private static class Visitor extends AnalyzedRelationVisitor<ConsumerContext, PlannedAnalyzedRelation> {

        @Override
        public PlannedAnalyzedRelation visitQueriedTable(QueriedTable table, ConsumerContext context) {
            if (table.querySpec().hasAggregates() || table.querySpec().groupBy()!=null) {
                return null;
            }
            TableInfo tableInfo = table.tableRelation().tableInfo();
            if (tableInfo.schemaInfo().systemSchema() || tableInfo.rowGranularity() != RowGranularity.DOC) {
                return null;
            }

            if(table.querySpec().where().hasVersions()){
                context.validationException(new VersionInvalidException());
                return null;
            }

            if (table.querySpec().where().noMatch()) {
                return new NoopPlannedAnalyzedRelation(table);
            }

            // TODO: if _score is selected, ordering by _score using a TopNProjection is only temporarily and MUST be changed.
            // Proposal: only order by _score if requested by user and order it on the shard
            boolean scoreSelected = false;
            ReferenceInfo docIdRefInfo = tableInfo.getReferenceInfo(DOC_ID_COLUMN_IDENT);
            List<Symbol> collectSymbols = Lists.<Symbol>newArrayList(new Reference(docIdRefInfo));

            List<Symbol> outputSymbols = new ArrayList<>(table.querySpec().outputs().size());
            for (Symbol symbol : table.querySpec().outputs()) {
                if (SCORE_REFERENCE_DETECTOR.detect(symbol)) {
                    scoreSelected = true;
                    collectSymbols.add(symbol);
                }
                outputSymbols.add(DocReferenceConverter.convertIfPossible(symbol, tableInfo));
            }

            OrderBy orderBy = table.querySpec().orderBy();
            if (orderBy != null) {
                table.tableRelation().validateOrderBy(orderBy);
                for (Symbol symbol : orderBy.orderBySymbols()) {
                    if (!collectSymbols.contains(symbol)) {
                        // order by symbols will be resolved on collect
                        collectSymbols.add(symbol);
                    }
                }
            }

            CollectNode collectNode = PlanNodeBuilder.collect(
                    tableInfo,
                    context.plannerContext(),
                    table.querySpec().where(),
                    collectSymbols,
                    ImmutableList.<Projection>of(),
                    orderBy,
                    MoreObjects.firstNonNull(table.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT) + table.querySpec().offset()
            );
            collectNode.keepContextForFetcher(true);

            if (orderBy != null) {
                MergeProjection mergeProjection = new MergeProjection(
                        collectSymbols,
                        orderBy.orderBySymbols(),
                        orderBy.reverseFlags(),
                        orderBy.nullsFirst());
                collectNode.projections(ImmutableList.<Projection>of(mergeProjection));
            }

            List<Projection> mergeProjections = new ArrayList<>();
            if (table.querySpec().isLimited() || scoreSelected) {
                TopNProjection topNProjection = new TopNProjection(
                        MoreObjects.firstNonNull(table.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT),
                        table.querySpec().offset(),
                        scoreSelected ? Arrays.<Symbol>asList(DEFAULT_SCORE_INPUT_COLUMN) : ImmutableList.<Symbol>of(),
                        scoreSelected ? new boolean[]{true} : new boolean[0],
                        scoreSelected ? new Boolean[]{false} : new Boolean[0]
                        );
                List<Symbol> outputs = new ArrayList<>();
                outputs.add(DEFAULT_DOC_ID_INPUT_COLUMN);
                if (scoreSelected) {
                    outputs.add(DEFAULT_SCORE_INPUT_COLUMN);
                }
                topNProjection.outputs(outputs);
                mergeProjections.add(topNProjection);
            }

            FetchProjection fetchProjection = new FetchProjection(
                    DEFAULT_DOC_ID_INPUT_COLUMN, collectSymbols, outputSymbols,
                    tableInfo.partitionedByColumns(),
                    collectNode.executionNodes(),
                    table.querySpec().isLimited());
            mergeProjections.add(fetchProjection);

            MergeNode localMergeNode;
            if (orderBy != null) {
                localMergeNode = PlanNodeBuilder.sortedLocalMerge(
                        mergeProjections,
                        orderBy,
                        collectSymbols,
                        null,
                        collectNode,
                        context.plannerContext());
            } else {
                localMergeNode = PlanNodeBuilder.localMerge(
                        mergeProjections,
                        collectNode,
                        context.plannerContext());
            }

            return new QueryThenFetch(collectNode, localMergeNode);
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }
    }

}
