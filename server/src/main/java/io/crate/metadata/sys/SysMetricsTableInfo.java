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

package io.crate.metadata.sys;

import static io.crate.types.DataTypes.DOUBLE;
import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.STRING_ARRAY;

import java.util.List;
import java.util.function.Supplier;

import org.elasticsearch.cluster.node.DiscoveryNode;

import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.SystemTable;

public class SysMetricsTableInfo {

    public static final RelationName NAME = new RelationName(SysSchemaInfo.NAME, "jobs_metrics");

    public static SystemTable<MetricsView> create(Supplier<DiscoveryNode> localNode) {
        return SystemTable.<MetricsView>builder(NAME)
            .add("total_count", LONG, MetricsView::totalCount)
            .add("total_affected_row_count", LONG, MetricsView::affectedRowCount)
            .add("sum_of_durations", LONG, MetricsView::sumOfDurations)
            .add("failed_count", LONG, MetricsView::failedCount)
            .add("mean", DOUBLE, MetricsView::mean)
            .add("stdev", DOUBLE, MetricsView::stdDeviation)
            .add("max", LONG, MetricsView::maxValue)
            .add("min", LONG, MetricsView::minValue)
            .startObject("percentiles")
                .add("25", LONG, x -> x.getValueAtPercentile(25.0))
                .add("50", LONG, x -> x.getValueAtPercentile(50.0))
                .add("75", LONG, x -> x.getValueAtPercentile(75.0))
                .add("90", LONG, x -> x.getValueAtPercentile(90.0))
                .add("95", LONG, x -> x.getValueAtPercentile(95.0))
                .add("99", LONG, x -> x.getValueAtPercentile(99.0))
            .endObject()
            .startObject("node")
                .add("id", STRING, ignored -> localNode.get().getId())
                .add("name", STRING, ignored -> localNode.get().getName())
            .endObject()
            .startObject("classification")
                .add("type", STRING, x -> x.classification().type().name())
                .add("labels", STRING_ARRAY, x -> List.copyOf(x.classification().labels()))
            .endObject()
            .withRouting((state, routingProvider, sessionSettings) -> Routing.forTableOnAllNodes(NAME, state.nodes()))
            .build();
    }
}
