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

package io.crate.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class IndexPartsTest {

    @Test
    public void testParsing() {
        String table = "table";
        String schemaTable = "schema.table";
        String ident = "ident";
        String partitionedTable = ".partitioned.table." + ident;
        String schemaPartitionedTable = "schema..partitioned.table." + ident;

        assertThat(IndexName.decode(table).schema()).isEqualTo(Schemas.DOC_SCHEMA_NAME);
        assertThat(IndexName.decode(schemaTable).schema()).isEqualTo("schema");
        assertThat(IndexName.decode(partitionedTable).schema()).isEqualTo(Schemas.DOC_SCHEMA_NAME);
        assertThat(IndexName.decode(schemaPartitionedTable).schema()).isEqualTo("schema");

        assertThat(IndexName.decode(table).table()).isEqualTo(table);
        assertThat(IndexName.decode(schemaTable).table()).isEqualTo(table);
        assertThat(IndexName.decode(partitionedTable).table()).isEqualTo(table);
        assertThat(IndexName.decode(schemaPartitionedTable).table()).isEqualTo(table);

        assertThat(IndexName.decode(table).isPartitioned()).isFalse();
        assertThat(IndexName.decode(schemaTable).isPartitioned()).isFalse();
        assertThat(IndexName.decode(partitionedTable).isPartitioned()).isTrue();
        assertThat(IndexName.decode(schemaPartitionedTable).isPartitioned()).isTrue();
        assertThatThrownBy(() -> IndexName.decode("schema..partitioned."))
            .as("Should have failed due to invalid index name")
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid index name: schema");

        assertThat(IndexName.isPartitioned(table)).isFalse();
        assertThat(IndexName.isPartitioned(schemaTable)).isFalse();
        assertThat(IndexName.isPartitioned(partitionedTable)).isTrue();
        assertThat(IndexName.isPartitioned(schemaPartitionedTable)).isTrue();
        assertThat(IndexName.isPartitioned("schema..partitioned.")).isFalse();
        assertThat(IndexName.isPartitioned("schema.partitioned.")).isFalse();
        assertThat(IndexName.isPartitioned("schema..partitioned.t")).isTrue();

        assertThat(IndexName.decode(table).partitionIdent()).isEqualTo("");
        assertThat(IndexName.decode(schemaTable).partitionIdent()).isEqualTo("");
        assertThat(IndexName.decode(partitionedTable).partitionIdent()).isEqualTo(ident);
        assertThat(IndexName.decode(schemaPartitionedTable).partitionIdent()).isEqualTo(ident);

        assertThat(IndexName.isDangling(table)).isFalse();
        assertThat(IndexName.isDangling(schemaTable)).isFalse();
        assertThat(IndexName.isDangling(partitionedTable)).isFalse();
        assertThat(IndexName.isDangling(schemaPartitionedTable)).isFalse();
        assertThat(IndexName.isDangling("schema..partitioned.")).isFalse();
        assertThat(IndexName.isDangling("schema.partitioned.")).isFalse();
        assertThat(IndexName.isDangling("schema..partitioned.t")).isFalse();
        assertThat(IndexName.isDangling(".shrinked.t")).isTrue();
        assertThat(IndexName.isDangling(".shrinked.schema.t")).isTrue();
        assertThat(IndexName.isDangling(".shrinked.partitioned.t.ident")).isTrue();
        assertThat(IndexName.isDangling(".shrinked.schema..partitioned.t.ident")).isTrue();
    }

}
