/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.node.ddl;

import static io.crate.blob.v2.BlobIndex.fullIndexName;
import static io.crate.blob.v2.BlobIndicesService.SETTING_INDEX_BLOBS_ENABLED;

import java.util.function.Function;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;

import io.crate.analyze.AnalyzedCreateBlobTable;
import io.crate.analyze.NumberOfShards;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.TableParameter;
import io.crate.analyze.TableParameters;
import io.crate.analyze.TableProperties;
import org.jetbrains.annotations.VisibleForTesting;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.CreateBlobTable;
import io.crate.sql.tree.GenericProperties;

public class CreateBlobTablePlan implements Plan {

    private final AnalyzedCreateBlobTable analyzedBlobTable;
    private final NumberOfShards numberOfShards;

    public CreateBlobTablePlan(AnalyzedCreateBlobTable analyzedBlobTable, NumberOfShards numberOfShards) {
        this.analyzedBlobTable = analyzedBlobTable;
        this.numberOfShards = numberOfShards;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) throws Exception {
        RelationName relationName = analyzedBlobTable.relationName();
        Settings settings = buildSettings(
            analyzedBlobTable.createBlobTable(),
            plannerContext.transactionContext(),
            dependencies.nodeContext(),
            params,
            subQueryResults,
            numberOfShards);

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(fullIndexName(relationName.name()), settings);

        OneRowActionListener<CreateIndexResponse> listener =
            new OneRowActionListener<>(consumer, r -> new Row1(1L));
        dependencies.createIndexAction().execute(createIndexRequest).whenComplete(listener);
    }

    @VisibleForTesting
    public static Settings buildSettings(CreateBlobTable<Symbol> createBlobTable,
                                         CoordinatorTxnCtx txnCtx,
                                         NodeContext nodeCtx,
                                         Row params,
                                         SubQueryResults subQueryResults,
                                         NumberOfShards numberOfShards) {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            txnCtx,
            nodeCtx,
            x,
            params,
            subQueryResults
        );
        CreateBlobTable<Object> blobTable = createBlobTable.map(eval);
        GenericProperties<Object> properties = blobTable.genericProperties();

        // apply default in case it is not specified in the properties,
        // if it is it will get overwritten afterwards.
        TableParameter tableParameter = new TableParameter();
        TableProperties.analyze(
            tableParameter,
            TableParameters.CREATE_BLOB_TABLE_PARAMETERS,
            properties,
            true
        );
        Settings.Builder builder = Settings.builder();
        builder.put(tableParameter.settings());
        builder.put(SETTING_INDEX_BLOBS_ENABLED.getKey(), true);

        int numShards;
        ClusteredBy<Object> clusteredBy = blobTable.clusteredBy();
        if (clusteredBy != null) {
            numShards = numberOfShards.fromClusteredByClause(clusteredBy);
        } else {
            numShards = numberOfShards.defaultNumberOfShards();
        }
        builder.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards);

        return builder.build();
    }
}
