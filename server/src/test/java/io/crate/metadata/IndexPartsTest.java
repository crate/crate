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

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;

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

        assertThat(new IndexParts(table).isPartitioned(), is(false));
        assertThat(new IndexParts(schemaTable).isPartitioned(), is(false));
        assertThat(new IndexParts(partitionedTable).isPartitioned(), is(true));
        assertThat(new IndexParts(schemaPartitionedTable).isPartitioned(), is(true));
        try {
            new IndexParts("schema..partitioned.");
            fail("Should have failed due to invalid index name");
        } catch (IllegalArgumentException ignored) {}

        assertThat(IndexParts.isPartitioned(table), is(false));
        assertThat(IndexParts.isPartitioned(schemaTable), is(false));
        assertThat(IndexParts.isPartitioned(partitionedTable), is(true));
        assertThat(IndexParts.isPartitioned(schemaPartitionedTable), is(true));
        assertThat(IndexParts.isPartitioned("schema..partitioned."), is(false));
        assertThat(IndexParts.isPartitioned("schema.partitioned."), is(false));
        assertThat(IndexParts.isPartitioned("schema..partitioned.t"), is(true));

        assertThat(new IndexParts(table).getPartitionIdent(), is(""));
        assertThat(new IndexParts(schemaTable).getPartitionIdent(), is(""));
        assertThat(new IndexParts(partitionedTable).getPartitionIdent(), is(ident));;
        assertThat(new IndexParts(schemaPartitionedTable).getPartitionIdent(), is(ident));

        assertThat(IndexParts.isDangling(table), is(false));
        assertThat(IndexParts.isDangling(schemaTable), is(false));
        assertThat(IndexParts.isDangling(partitionedTable), is(false));
        assertThat(IndexParts.isDangling(schemaPartitionedTable), is(false));
        assertThat(IndexParts.isDangling("schema..partitioned."), is(false));
        assertThat(IndexParts.isDangling("schema.partitioned."), is(false));
        assertThat(IndexParts.isDangling("schema..partitioned.t"), is(false));
        assertThat(IndexParts.isDangling(".shrinked.t"), is(true));
        assertThat(IndexParts.isDangling(".shrinked.schema.t"), is(true));
        assertThat(IndexParts.isDangling(".shrinked.partitioned.t.ident"), is(true));
        assertThat(IndexParts.isDangling(".shrinked.schema..partitioned.t.ident"), is(true));
    }

}
