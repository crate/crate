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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import org.junit.Test;

public class IndexPartsTest {

    @Test
    public void testParsing() {
        String table = "table";
        String schemaTable = "schema.table";
        String ident = "ident";
        String partitionedTable = ".partitioned.table." + ident;
        String schemaPartitionedTable = "schema..partitioned.table." + ident;

        assertThat(new IndexParts(table).getSchema(), is(Schemas.DOC_SCHEMA_NAME));
        assertThat(new IndexParts(schemaTable).getSchema(), is("schema"));
        assertThat(new IndexParts(partitionedTable).getSchema(), is(Schemas.DOC_SCHEMA_NAME));
        assertThat(new IndexParts(schemaPartitionedTable).getSchema(), is("schema"));

        assertThat(new IndexParts(table).getTable(), is(table));
        assertThat(new IndexParts(schemaTable).getTable(), is(table));
        assertThat(new IndexParts(partitionedTable).getTable(), is(table));
        assertThat(new IndexParts(schemaPartitionedTable).getTable(), is(table));

        assertThat(new IndexParts(table).isPartitioned()).isFalse();
        assertThat(new IndexParts(schemaTable).isPartitioned()).isFalse();
        assertThat(new IndexParts(partitionedTable).isPartitioned()).isTrue();
        assertThat(new IndexParts(schemaPartitionedTable).isPartitioned()).isTrue();
        assertThatThrownBy(() -> new IndexParts("schema..partitioned."))
            .as("Should have failed due to invalid index name")
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid index name: schema");

        assertThat(IndexParts.isPartitioned(table)).isFalse();
        assertThat(IndexParts.isPartitioned(schemaTable)).isFalse();
        assertThat(IndexParts.isPartitioned(partitionedTable)).isTrue();
        assertThat(IndexParts.isPartitioned(schemaPartitionedTable)).isTrue();
        assertThat(IndexParts.isPartitioned("schema..partitioned.")).isFalse();
        assertThat(IndexParts.isPartitioned("schema.partitioned.")).isFalse();
        assertThat(IndexParts.isPartitioned("schema..partitioned.t")).isTrue();

        assertThat(new IndexParts(table).getPartitionIdent(), is(""));
        assertThat(new IndexParts(schemaTable).getPartitionIdent(), is(""));
        assertThat(new IndexParts(partitionedTable).getPartitionIdent(), is(ident));
        assertThat(new IndexParts(schemaPartitionedTable).getPartitionIdent(), is(ident));

        assertThat(IndexParts.isDangling(table)).isFalse();
        assertThat(IndexParts.isDangling(schemaTable)).isFalse();
        assertThat(IndexParts.isDangling(partitionedTable)).isFalse();
        assertThat(IndexParts.isDangling(schemaPartitionedTable)).isFalse();
        assertThat(IndexParts.isDangling("schema..partitioned.")).isFalse();
        assertThat(IndexParts.isDangling("schema.partitioned.")).isFalse();
        assertThat(IndexParts.isDangling("schema..partitioned.t")).isFalse();
        assertThat(IndexParts.isDangling(".shrinked.t")).isTrue();
        assertThat(IndexParts.isDangling(".shrinked.schema.t")).isTrue();
        assertThat(IndexParts.isDangling(".shrinked.partitioned.t.ident")).isTrue();
        assertThat(IndexParts.isDangling(".shrinked.schema..partitioned.t.ident")).isTrue();
    }

}
