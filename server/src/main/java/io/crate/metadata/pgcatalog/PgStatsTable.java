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

package io.crate.metadata.pgcatalog;

import java.util.ArrayList;

import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.statistics.ColumnStatsEntry;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

public final class PgStatsTable {

    public static final RelationName NAME = new RelationName(PgCatalogSchemaInfo.NAME, "pg_stats");

    private PgStatsTable() {}

    public static SystemTable<ColumnStatsEntry> INSTANCE = SystemTable.<ColumnStatsEntry>builder(NAME)
        .add("schemaname", DataTypes.STRING, x -> x.relation().schema())
        .add("tablename", DataTypes.STRING, x -> x.relation().name())
        .add("attname", DataTypes.STRING, x -> x.column().sqlFqn())
        .add("inherited", DataTypes.BOOLEAN, c -> false)
        .add("null_frac", DataTypes.FLOAT, x -> (float) x.columnStats().nullFraction())
        .add("avg_width", DataTypes.INTEGER, x -> (int) x.columnStats().averageSizeInBytes())
        .add("n_distinct", DataTypes.FLOAT, x -> (float) x.columnStats().approxDistinct())

        // The arrays have the `anyarray` type in PostgreSQL, which are pseudo-types / polymorphic types
        // (their actual type depends on in which context the columns are used)
        // See https://www.postgresql.org/docs/current/extend-type-system.html
        // We lack the capabilities to decide "on-use" which type to use, so we use a string array as most types can be casted to string.
        .add(
            "most_common_vals",
            DataTypes.STRING_ARRAY,
            x -> DataTypes.STRING_ARRAY.fromAnyArray(x.columnStats().mostCommonValues().values())
        )
        .add(
            "most_common_freqs",
            DataTypes.FLOAT_ARRAY,
            x -> {
                double[] frequencies = x.columnStats().mostCommonValues().frequencies();
                ArrayList<Float> values = new ArrayList<>(frequencies.length);
                for (double frequency : frequencies) {
                    values.add((float) frequency);
                }
                return values;
            }
        )
        .add(
            "histogram_bounds",
            DataTypes.STRING_ARRAY,
            x -> DataTypes.STRING_ARRAY.fromAnyArray(x.columnStats().histogram())
        )
        .add("correlation", DataTypes.FLOAT, c -> 0.0f)
        .add("most_common_elems", DataTypes.STRING_ARRAY, c -> null)
        .add("most_common_elem_freqs", new ArrayType<>(DataTypes.FLOAT), c -> null)
        .add("elem_count_histogram", new ArrayType<>(DataTypes.FLOAT), c -> null)
        .build();
}
