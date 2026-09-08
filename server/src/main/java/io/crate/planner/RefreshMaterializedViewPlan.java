/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package io.crate.planner;

import static org.elasticsearch.cluster.metadata.Metadata.OID_UNASSIGNED;

import java.util.List;
import java.util.stream.Collectors;

import io.crate.analyze.AnalyzedRefreshMaterializedView;
import io.crate.analyze.NumberOfShards;
import io.crate.data.CollectingRowConsumer;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.data.SentinelRow;
import io.crate.execution.ddl.RelationNameSwap;
import io.crate.execution.ddl.SwapRelationsRequest;
import io.crate.execution.ddl.TransportSwapRelations;
import io.crate.execution.ddl.tables.CreateTableClient;
import io.crate.execution.ddl.tables.DropTableRequest;
import io.crate.execution.ddl.tables.TransportDropTable;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.consumer.CreateTableAsPlan;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.operators.SubQueryResults;

public final class RefreshMaterializedViewPlan implements Plan {

    private final RelationName target;
    private final int targetOid;
    private final RelationName replacement;
    private final CreateTableAsPlan createReplacement;

    public static RefreshMaterializedViewPlan of(AnalyzedRefreshMaterializedView analysis,
                                                 NumberOfShards numberOfShards,
                                                 CreateTableClient tableCreator,
                                                 PlannerContext context,
                                                 LogicalPlanner logicalPlanner) {
        return new RefreshMaterializedViewPlan(
            analysis.target().ident(),
            analysis.target().oid(),
            analysis.replacement().analyzedCreateTable().relationName(),
            CreateTableAsPlan.of(
                analysis.replacement(),
                numberOfShards,
                tableCreator,
                context,
                logicalPlanner
            )
        );
    }

    private RefreshMaterializedViewPlan(RelationName target,
                                        int targetOid,
                                        RelationName replacement,
                                        CreateTableAsPlan createReplacement) {
        this.target = target;
        this.targetOid = targetOid;
        this.replacement = replacement;
        this.createReplacement = createReplacement;
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
                              SubQueryResults subQueryResults) {
        CollectingRowConsumer<?, Long> createConsumer = new CollectingRowConsumer<>(Collectors.counting());
        createConsumer.completionFuture().whenComplete((_, createError) -> {
            if (createError != null) {
                cleanupAndFail(dependencies, consumer, createError);
                return;
            }
            SwapRelationsRequest request;
            try {
                DocTableInfo replacementTable = dependencies.schemas().getTableInfo(replacement);
                request = new SwapRelationsRequest(
                    List.of(new RelationNameSwap(replacement, replacementTable.oid(), target, targetOid)),
                    List.of(replacement)
                );
            } catch (Throwable t) {
                cleanupAndFail(dependencies, consumer, t);
                return;
            }
            dependencies.client().execute(TransportSwapRelations.ACTION, request).whenComplete((response, swapError) -> {
                if (swapError != null) {
                    cleanupAndFail(dependencies, consumer, swapError);
                } else {
                    consumer.accept(
                        InMemoryBatchIterator.of(
                            new io.crate.data.Row1(response.isAcknowledged() ? 1L : 0L),
                            SentinelRow.SENTINEL
                        ),
                        null
                    );
                }
            });
        });
        Plan.execute(
            createReplacement,
            dependencies,
            plannerContext,
            createConsumer,
            params,
            subQueryResults
        );
    }

    private void cleanupAndFail(DependencyCarrier dependencies,
                                RowConsumer consumer,
                                Throwable failure) {
        dependencies.client().execute(
            TransportDropTable.ACTION,
            new DropTableRequest(replacement, OID_UNASSIGNED)
        ).whenComplete((_, _) -> consumer.accept(null, failure));
    }
}
