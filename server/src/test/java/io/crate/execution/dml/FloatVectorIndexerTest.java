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

package io.crate.execution.dml;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.types.FloatVectorType;

public class FloatVectorIndexerTest {

    @Test
    public void test_indexer_has_default_fieldtype() throws Exception {
        FloatVectorType type = new FloatVectorType(4);
        RelationName tableName = new RelationName("doc", "tbl");
        Reference ref = new SimpleReference(
            new ReferenceIdent(tableName, "x"),
            RowGranularity.DOC,
            type,
            1,
            null
        );
        FloatVectorIndexer floatVectorIndexer = new FloatVectorIndexer(ref, null);
        assertThat(floatVectorIndexer.fieldType).isNotNull();
        assertThat(floatVectorIndexer.fieldType.vectorDimension()).isEqualTo(4);
    }

    @Test
    public void test_indexer_with_2048_dimensions() throws Exception {
        FloatVectorType type = new FloatVectorType(2048);
        RelationName tableName = new RelationName("doc", "tbl");
        Reference ref = new SimpleReference(
            new ReferenceIdent(tableName, "x"),
            RowGranularity.DOC,
            type,
            1,
            null
        );
        FloatVectorIndexer floatVectorIndexer = new FloatVectorIndexer(ref, null);
        assertThat(floatVectorIndexer.fieldType).isNotNull();
        assertThat(floatVectorIndexer.fieldType.vectorDimension()).isEqualTo(2048);
    }

}
