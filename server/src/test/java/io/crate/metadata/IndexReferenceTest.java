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

import static io.crate.metadata.ReferenceTest.columnMapping;
import static io.crate.testing.Asserts.assertThat;

import java.util.List;
import java.util.Map;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.StringType;

public class IndexReferenceTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testStreaming() throws Exception {
        RelationName relationName = new RelationName("doc", "test");
        ReferenceIdent referenceIdent = new ReferenceIdent(relationName, "string_col");
        SimpleReference reference = new SimpleReference(referenceIdent, RowGranularity.DOC, StringType.INSTANCE, 1, null);

        ReferenceIdent indexReferenceIdent = new ReferenceIdent(relationName, "index_column");
        IndexReference indexReferenceInfo = new IndexReference(
            2,
            123,
            true,
            indexReferenceIdent,
            IndexType.FULLTEXT, List.of(reference), "my_analyzer");

        BytesStreamOutput out = new BytesStreamOutput();
        Reference.toStream(out, indexReferenceInfo);

        StreamInput in = out.bytes().streamInput();
        IndexReference indexReferenceInfo2 = Reference.fromStream(in);

        assertThat(indexReferenceInfo2).isEqualTo(indexReferenceInfo);
    }

    @Test
    public void test_mapping_generation_ft_col_has_source_columns() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (title string, description string, " +
                      "index title_desc_fulltext using fulltext(title, description) " +
                      "with (analyzer = 'stop'))"
            ).build();

        DocTableInfo table = e.resolveTableInfo("tbl");
        IndexReference reference = table.indexColumn(new ColumnIdent("title_desc_fulltext"));

        Map<String, Object> mapping = reference.toMapping(reference.position(), null);
        assertThat(mapping)
            .containsEntry("sources", List.of("title", "description"))
            .containsEntry("oid", 3L)
            .containsEntry("analyzer", "stop");
        IndexMetadata indexMetadata = clusterService.state().metadata().indices().valuesIt().next();
        Map<String, Object> sourceAsMap = indexMetadata.mapping().sourceAsMap();
        assertThat(columnMapping(sourceAsMap, "properties.title_desc_fulltext")).isEqualTo(mapping);
    }


}
