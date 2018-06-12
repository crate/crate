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

import com.google.common.collect.ImmutableMap;
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
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;

public class SysMetricsTableInfo extends StaticTableInfo {

    public static final RelationName NAME = new RelationName(SysSchemaInfo.NAME, "jobs_metrics");

    static class Columns {
        static final ColumnIdent TOTAL_COUNT = new ColumnIdent("total_count");
        static final ColumnIdent MEAN = new ColumnIdent("mean");
        static final ColumnIdent STDEV = new ColumnIdent("stdev");
        static final ColumnIdent MAX = new ColumnIdent("max");
        static final ColumnIdent MIN = new ColumnIdent("min");
        static final ColumnIdent PERCENTILES = new ColumnIdent("percentiles");
        static final ColumnIdent P25 = new ColumnIdent("percentiles", "25");
        static final ColumnIdent P50 = new ColumnIdent("percentiles", "50");
        static final ColumnIdent P75 = new ColumnIdent("percentiles", "75");
        static final ColumnIdent P90 = new ColumnIdent("percentiles", "90");
        static final ColumnIdent P95 = new ColumnIdent("percentiles", "95");
        static final ColumnIdent P99 = new ColumnIdent("percentiles", "99");
        static final ColumnIdent NODE = new ColumnIdent("node");
        static final ColumnIdent NODE_ID = new ColumnIdent("node", "id");
        static final ColumnIdent NODE_NAME = new ColumnIdent("node", "name");
        static final ColumnIdent CLASS = new ColumnIdent("classification");
        static final ColumnIdent CLASS_TYPE = new ColumnIdent("classification", "type");
        static final ColumnIdent CLASS_LABELS = new ColumnIdent("classification", "labels");
    }

    SysMetricsTableInfo() {
        super(NAME,
            new ColumnRegistrar(NAME, RowGranularity.DOC)
                .register(Columns.TOTAL_COUNT, DataTypes.LONG)
                .register(Columns.MEAN, DataTypes.DOUBLE)
                .register(Columns.STDEV, DataTypes.DOUBLE)
                .register(Columns.MAX, DataTypes.LONG)
                .register(Columns.MIN, DataTypes.LONG)
                .register(Columns.PERCENTILES, DataTypes.OBJECT)
                .register(Columns.P25, DataTypes.LONG)
                .register(Columns.P50, DataTypes.LONG)
                .register(Columns.P75, DataTypes.LONG)
                .register(Columns.P90, DataTypes.LONG)
                .register(Columns.P95, DataTypes.LONG)
                .register(Columns.P99, DataTypes.LONG)
                .register(Columns.NODE, DataTypes.OBJECT)
                .register(Columns.NODE_ID, DataTypes.STRING)
                .register(Columns.NODE_NAME, DataTypes.STRING)
                .register(Columns.CLASS, DataTypes.OBJECT)
                .register(Columns.CLASS_TYPE, DataTypes.STRING)
                .register(Columns.CLASS_LABELS, DataTypes.STRING_ARRAY),
            Collections.emptyList()
        );
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<ClassifiedHistograms.ClassifiedHistogram>> expressions(Supplier<DiscoveryNode> localNode) {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<ClassifiedHistograms.ClassifiedHistogram>>builder()
            .put(Columns.TOTAL_COUNT, () -> forFunction(h -> h.histogram().getTotalCount()))
            .put(Columns.MEAN, () -> forFunction(h -> h.histogram().getMean()))
            .put(Columns.STDEV, () -> forFunction(h -> h.histogram().getStdDeviation()))
            .put(Columns.MAX, () -> forFunction(h -> h.histogram().getMaxValue()))
            .put(Columns.MIN, () -> forFunction(h -> h.histogram().getMinValue() == Long.MAX_VALUE ? 0L : h.histogram().getMinValue()))
            .put(Columns.PERCENTILES, () -> forFunction(h -> ImmutableMap.builder()
                .put("25", h.histogram().getValueAtPercentile(25.0))
                .put("50", h.histogram().getValueAtPercentile(50.0))
                .put("75", h.histogram().getValueAtPercentile(75.0))
                .put("90", h.histogram().getValueAtPercentile(90.0))
                .put("95", h.histogram().getValueAtPercentile(95.0))
                .put("99", h.histogram().getValueAtPercentile(99.0))
                .build()
            ))
            .put(Columns.P25, () -> forFunction(h -> h.histogram().getValueAtPercentile(25.0)))
            .put(Columns.P50, () -> forFunction(h -> h.histogram().getValueAtPercentile(50.0)))
            .put(Columns.P75, () -> forFunction(h -> h.histogram().getValueAtPercentile(75.0)))
            .put(Columns.P90, () -> forFunction(h -> h.histogram().getValueAtPercentile(90.0)))
            .put(Columns.P95, () -> forFunction(h -> h.histogram().getValueAtPercentile(95.0)))
            .put(Columns.P99, () -> forFunction(h -> h.histogram().getValueAtPercentile(99.0)))
            .put(Columns.NODE, () -> forFunction(ignored -> ImmutableMap.builder()
                .put("id", new BytesRef(localNode.get().getId()))
                .put("name", new BytesRef(localNode.get().getName()))
                .build()
            ))
            .put(Columns.NODE_ID, () -> forFunction(ignored -> new BytesRef(localNode.get().getId())))
            .put(Columns.NODE_NAME, () -> forFunction(ignored -> new BytesRef(localNode.get().getName())))
            .put(Columns.CLASS, () -> forFunction(h -> ImmutableMap.builder()
                .put("type", new BytesRef(h.classification().type().name()))
                .put("labels", h.classification().labels()
                    .stream()
                    .map(BytesRefs::toBytesRef)
                    .toArray(BytesRef[]::new)
                )
                .build()
            ))
            .put(Columns.CLASS_TYPE, () -> forFunction(h -> new BytesRef(h.classification().type().name())))
            .put(Columns.CLASS_LABELS, () -> forFunction(h -> h.classification().labels()
                .stream()
                .map(BytesRefs::toBytesRef)
                .toArray(BytesRef[]::new)
            ))
            .build();
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
