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

import java.util.List;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.blob.v2.BlobIndex;
import io.crate.exceptions.InvalidRelationName;
import io.crate.exceptions.InvalidSchemaNameException;

public class RelationNameTest extends ESTestCase {

    @Test
    public void testIndexName() throws Exception {
        RelationName ti = new RelationName(Schemas.DOC_SCHEMA_NAME, "t");
        assertThat(ti.indexNameOrAlias()).isEqualTo("t");
        ti = new RelationName("s", "t");
        assertThat(ti.indexNameOrAlias()).isEqualTo("s.t");
    }

    @Test
    public void testFromIndexName() throws Exception {
        assertThat(RelationName.fromIndexName("t")).isEqualTo(new RelationName(Schemas.DOC_SCHEMA_NAME, "t"));
        assertThat(RelationName.fromIndexName("s.t")).isEqualTo(new RelationName("s", "t"));

        PartitionName pn = new PartitionName(new RelationName("s", "t"), List.of("v1"));
        assertThat(RelationName.fromIndexName(pn.asIndexName())).isEqualTo(new RelationName("s", "t"));

        pn = new PartitionName(new RelationName("doc", "t"), List.of("v1"));
        assertThat(RelationName.fromIndexName(pn.asIndexName())).isEqualTo(new RelationName(Schemas.DOC_SCHEMA_NAME, "t"));
    }

    @Test
    public void testFromIndexNameCreatesCorrectBlobRelationName() {
        RelationName relationName = new RelationName("blob", "foobar");
        String indexName = relationName.indexNameOrAlias();
        assertThat(BlobIndex.isBlobIndex(indexName)).isTrue();
        assertThat(RelationName.fromIndexName(indexName)).isEqualTo(relationName);
    }

    @Test
    public void testDefaultSchema() throws Exception {
        RelationName ti = new RelationName(Schemas.DOC_SCHEMA_NAME, "t");
        assertThat(ti.schema()).isEqualTo("doc");
        assertThat(ti).isEqualTo(new RelationName("doc", "t"));
    }

    @Test
    public void testFQN() throws Exception {
        RelationName ti = new RelationName(Schemas.DOC_SCHEMA_NAME, "t");
        assertThat(ti.fqn()).isEqualTo("doc.t");

        ti = new RelationName("s", "t");
        assertThat(ti.fqn()).isEqualTo("s.t");
    }

    @Test
    public void testFqnFromIndexName() throws Exception {
        assertThat(RelationName.fqnFromIndexName("t1")).isEqualTo(Schemas.DOC_SCHEMA_NAME + ".t1");
        assertThat(RelationName.fqnFromIndexName("my_schema.t1")).isEqualTo("my_schema.t1");
        assertThat(RelationName.fqnFromIndexName(".partitioned.t1.abc")).isEqualTo(Schemas.DOC_SCHEMA_NAME + ".t1");
        assertThat(RelationName.fqnFromIndexName("my_schema..partitioned.t1.abc")).isEqualTo("my_schema.t1");
    }

    @Test
    public void test__all_cannot_be_used() throws Exception {
        // conflicts with `_all` wildcard, causing havoc in some operations ("drop table _all" would delete _all_ tables)

        RelationName relationName = new RelationName("doc", "_all");
        assertThatThrownBy(() -> relationName.ensureValidForRelationCreation())
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("\"_all\" cannot be used as schema or table name");
    }

    @Test
    public void testFqnFromIndexNameUnsupported3Parts() throws Exception {
        assertThatThrownBy(() -> RelationName.fqnFromIndexName("my_schema.t1.foo"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid index name: my_schema.t1.foo");
    }

    @Test
    public void testFqnFromIndexNameUnsupportedMoreThan5Parts() throws Exception {
        assertThatThrownBy(() -> RelationName.fqnFromIndexName("my_schema..partitioned.t1.abc.foo"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid index name: my_schema..partitioned.t1.abc.foo");
    }

    @Test
    public void testCreateRelationNameWithInvalidCharacters() {
        assertThatThrownBy(() -> new RelationName("doc", ".table"))
            .isExactlyInstanceOf(InvalidRelationName.class)
            .hasMessage("Relation name \"doc..table\" is invalid.");
        assertThatThrownBy(() -> new RelationName(null, ".table"))
            .isExactlyInstanceOf(InvalidRelationName.class)
            .hasMessage("Relation name \".table\" is invalid.");
        assertThatThrownBy(() -> new RelationName("doc.", ".table"))
            .isExactlyInstanceOf(InvalidSchemaNameException.class)
            .hasMessage("schema name \"doc.\" is invalid.");
    }
}
