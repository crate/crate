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

package io.crate.metadata.pgcatalog;

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
import io.crate.statistics.ColumnStatsEntry;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterState;

import java.util.ArrayList;
import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.constant;
import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;

public class PgStatsTable extends StaticTableInfo<ColumnStatsEntry> {

    public static final RelationName NAME = new RelationName(PgCatalogSchemaInfo.NAME, "pg_stats");
    private static final ColumnRegistrar<ColumnStatsEntry> COLUMN_REGISTRY = columnRegistry();

    public PgStatsTable() {
        super(NAME, COLUMN_REGISTRY);
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<ColumnStatsEntry>> expressions() {
        return COLUMN_REGISTRY.expressions();
    }

    private static ColumnRegistrar<ColumnStatsEntry> columnRegistry() {
        return new ColumnRegistrar<ColumnStatsEntry>(NAME, RowGranularity.DOC)
            .register("schemaname", DataTypes.STRING, () -> forFunction(x -> x.relation().schema()))
            .register("tablename", DataTypes.STRING, () -> forFunction(x -> x.relation().name()))
            .register("attname", DataTypes.STRING, () -> forFunction(x -> x.column().sqlFqn()))
            .register("inherited", DataTypes.BOOLEAN, () -> constant(false))
            .register("null_frac", DataTypes.FLOAT, () -> forFunction(x -> (float) x.columnStats().nullFraction()))
            .register("avg_width", DataTypes.INTEGER, () -> forFunction(x -> (int) x.columnStats().averageSizeInBytes()))
            .register("n_distinct", DataTypes.FLOAT, () -> forFunction(x -> (float) x.columnStats().approxDistinct()))

            // The arrays have the `anyarray` type in PostgreSQL, which are pseudo-types / polymorphic types
            // (their actual type depends on in which context the columns are used)
            // See https://www.postgresql.org/docs/current/extend-type-system.html
            // We lack the capabilities to decide "on-use" which type to use, so we use a string array as most types can be casted to string.
            .register(
                "most_common_vals",
                DataTypes.STRING_ARRAY,
                () -> forFunction(x -> DataTypes.STRING_ARRAY
                    .fromAnyArray(x.columnStats().mostCommonValues().values()))
            )
            .register(
                "most_common_freqs",
                DataTypes.FLOAT_ARRAY,
                () -> forFunction(x -> {
                    double[] frequencies = x.columnStats().mostCommonValues().frequencies();
                    ArrayList<Float> values = new ArrayList<>(frequencies.length);
                    for (double frequency : frequencies) {
                        values.add((float) frequency);
                    }
                    return values;
                })
            )
            .register(
                "histogram_bounds",
                DataTypes.STRING_ARRAY,
                () -> forFunction(x -> DataTypes.STRING_ARRAY
                    .fromAnyArray(x.columnStats().histogram()))
            )
            .register("correlation", DataTypes.FLOAT, () -> constant(0.0f))
            .register("most_common_elems", DataTypes.STRING_ARRAY, () -> constant(null))
            .register("most_common_elem_freqs", new ArrayType<>(DataTypes.FLOAT), () -> constant(null))
            .register("elem_count_histogram", new ArrayType<>(DataTypes.FLOAT), () -> constant(null));
    }

    @Override
    public Routing getRouting(ClusterState state,
                              RoutingProvider routingProvider,
                              WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection,
                              SessionContext sessionContext) {
        return Routing.forTableOnSingleNode(NAME, state.getNodes().getLocalNodeId());
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }

}
