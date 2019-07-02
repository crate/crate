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

package io.crate.metadata.sys;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.ObjectType;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.Map;
import java.util.function.Supplier;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.DOUBLE;
import static io.crate.types.DataTypes.STRING_ARRAY;
import static io.crate.types.DataTypes.UNDEFINED;

public class SysMetricsTableInfo extends StaticTableInfo<MetricsView> {

    public static final RelationName NAME = new RelationName(SysSchemaInfo.NAME, "jobs_metrics");

    private static ColumnRegistrar<MetricsView> columnRegistrar(Supplier<DiscoveryNode> localNode) {
        return new ColumnRegistrar<MetricsView>(NAME, RowGranularity.DOC)
            .register("total_count", LONG, () -> forFunction(MetricsView::totalCount))
            .register("sum_of_durations", LONG, () -> forFunction(MetricsView::sumOfDurations))
            .register("failed_count", LONG, () -> forFunction(MetricsView::failedCount))
            .register("mean", DOUBLE, () -> forFunction(MetricsView::mean))
            .register("stdev", DOUBLE, () -> forFunction(MetricsView::stdDeviation))
            .register("max", LONG, () -> forFunction(MetricsView::maxValue))
            .register("min", LONG, () -> forFunction(MetricsView::minValue))
            .register("percentiles", ObjectType.builder()
                .setInnerType("25", LONG)
                .setInnerType("50", LONG)
                .setInnerType("75", LONG)
                .setInnerType("90", LONG)
                .setInnerType("95", LONG)
                .setInnerType("99", LONG)
                .build(), () -> forFunction(m -> Map.of(
                "25", m.getValueAtPercentile(25.0),
                "50", m.getValueAtPercentile(50.0),
                "75", m.getValueAtPercentile(75.0),
                "90", m.getValueAtPercentile(90.0),
                "95", m.getValueAtPercentile(95.0),
                "99", m.getValueAtPercentile(99.0)
                )
            ))
            .register("percentiles", "25", LONG, () -> forFunction(m -> m.getValueAtPercentile(25.0)))
            .register("percentiles", "50", LONG, () -> forFunction(m -> m.getValueAtPercentile(50.0)))
            .register("percentiles", "75", LONG, () -> forFunction(m -> m.getValueAtPercentile(75.0)))
            .register("percentiles", "90", LONG, () -> forFunction(m -> m.getValueAtPercentile(90.0)))
            .register("percentiles", "95", LONG, () -> forFunction(m -> m.getValueAtPercentile(95.0)))
            .register("percentiles", "99", LONG, () -> forFunction(m -> m.getValueAtPercentile(99.0)))

            .register("node", ObjectType.builder()
                .setInnerType("id", STRING)
                .setInnerType("name", STRING)
                .build(), () -> forFunction(ignored -> Map.of(
                    "id", localNode.get().getId(),
                    "name", localNode.get().getName()
                )

            ))
            .register("node", "id", STRING, () -> forFunction(ignored -> localNode.get().getId()))
            .register("node", "name", STRING, () -> forFunction(ignored -> localNode.get().getName()))
            .register("classification", ObjectType.builder()
                .setInnerType("type", STRING)
                .setInnerType("labels", STRING_ARRAY)
                .build(), () -> forFunction(h -> Map.of(
                 "type", h.classification().type().name(),
                 "labels", h.classification().labels().toArray(new String[0])
                )

            ))
            .register("classification", "type", STRING, () -> forFunction(h -> h.classification().type().name()))
            .register("classification", "labels", UNDEFINED, () -> forFunction(h -> h.classification().labels().toArray(new String[0])));
    }

    SysMetricsTableInfo(Supplier<DiscoveryNode> localNode) {
        super(NAME, columnRegistrar(localNode));
    }

    static Map<ColumnIdent, RowCollectExpressionFactory<MetricsView>> expressions(Supplier<DiscoveryNode> localNode) {
        return columnRegistrar(localNode).expressions();
    }

    @Override
    public Routing getRouting(ClusterState state, RoutingProvider routingProvider, WhereClause whereClause, RoutingProvider.ShardSelection shardSelection, SessionContext sessionContext) {
        return Routing.forTableOnAllNodes(NAME, state.getNodes());
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }
}
