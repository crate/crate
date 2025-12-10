/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataTypes;

public class TableStatsTest extends ESTestCase {

    private final RelationName testRelation = new RelationName(Schemas.DOC_SCHEMA_NAME, "test");
    private final SimpleReference idRef = new SimpleReference(
        testRelation,
        ColumnIdent.of("id"),
        RowGranularity.DOC,
        DataTypes.INTEGER,
        1,
        null);
    private final DocTableInfo docTableInfo = new DocTableInfo(
        testRelation,
        Map.of(idRef.column(), idRef),
        Map.of(),
        Set.of(),
        null,
        List.of(),
        List.of(),
        null,
        Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5)
            .build(),
        List.of(),
        ColumnPolicy.DYNAMIC,
        Version.CURRENT,
        null,
        false,
        Operation.ALL,
        0
    );

    @Test
    public void test_estimating_row_size_with_with_stats() {
        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(Map.of(docTableInfo.ident(), new Stats(1L, 1000L, Map.of()))::get);
        long sizeEstimate = tableStats.estimatedSizePerRow(docTableInfo.ident());
        assertThat(sizeEstimate).isEqualTo(1000L);
    }

    @Test
    public void test_estimating_row_size_with_empty_stats_using_columns_estimates() {
        TableStats tableStats = new TableStats();
        long sizeEstimate = tableStats.estimatedSizePerRow(docTableInfo);
        assertThat(sizeEstimate).isEqualTo(1168L);
    }

}
