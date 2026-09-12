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

import static org.apache.lucene.index.IndexWriter.MAX_TERM_LENGTH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.types.DataTypes;

public class StringIndexerTest {

    private static final RelationName RELATION = new RelationName("doc", "tbl");

    // Lucene only knows the storage identifier (oid) of a column, it must not leak into
    // user facing error messages. See https://github.com/crate/crate/issues/17376
    private static final long OID = 356;

    private static Reference textRef(ColumnIdent column, IndexType indexType, boolean hasDocValues) {
        return new SimpleReference(
            RELATION,
            column,
            RowGranularity.DOC,
            DataTypes.STRING,
            indexType,
            true,
            hasDocValues,
            1,
            OID,
            false,
            null
        );
    }

    private static void index(Reference ref, String value) throws Exception {
        IndexDocumentBuilder docBuilder = new IndexDocumentBuilder(
            TranslogWriter.wrapBytes(new BytesArray(new byte[0])),
            _ -> null,
            Map.of(),
            Version.CURRENT
        );
        new StringIndexer(ref).indexValue(value, docBuilder);
    }

    @Test
    public void test_too_large_value_for_doc_values_reports_column_name() {
        Reference ref = textRef(ColumnIdent.of("network"), IndexType.NONE, true);
        assertThatThrownBy(() -> index(ref, "a".repeat(MAX_TERM_LENGTH + 1)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Value for column \"network\" is too large, must be <= 32766 bytes "
                + "(got 32767 bytes). Use `INDEX OFF` and `STORAGE WITH (columnstore = false)` "
                + "to store larger values");
    }

    @Test
    public void test_too_large_value_for_indexed_column_reports_column_name() {
        Reference ref = textRef(ColumnIdent.of("network"), IndexType.PLAIN, false);
        assertThatThrownBy(() -> index(ref, "a".repeat(MAX_TERM_LENGTH + 1)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Value for column \"network\" is too large")
            .hasMessageNotContaining(String.valueOf(OID));
    }

    @Test
    public void test_error_uses_subscript_notation_for_children_of_objects() {
        Reference ref = textRef(ColumnIdent.of("o", List.of("network")), IndexType.NONE, true);
        assertThatThrownBy(() -> index(ref, "a".repeat(MAX_TERM_LENGTH + 1)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Value for column \"o['network']\" is too large");
    }

    @Test
    public void test_limit_is_measured_in_utf8_bytes() {
        Reference ref = textRef(ColumnIdent.of("network"), IndexType.NONE, true);
        // Each character requires 2 bytes in UTF-8, so the string is shorter than the
        // limit while its UTF-8 representation exceeds it
        String value = "\u00e4".repeat(MAX_TERM_LENGTH / 2 + 1);
        assertThat(value.length()).isLessThan(MAX_TERM_LENGTH);
        assertThatThrownBy(() -> index(ref, value))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("(got 32768 bytes)");
    }

    @Test
    public void test_value_at_the_limit_is_accepted() {
        Reference ref = textRef(ColumnIdent.of("network"), IndexType.NONE, true);
        assertThatCode(() -> index(ref, "a".repeat(MAX_TERM_LENGTH)))
            .doesNotThrowAnyException();
    }

    @Test
    public void test_too_large_value_is_accepted_without_index_and_doc_values() {
        Reference ref = textRef(ColumnIdent.of("network"), IndexType.NONE, false);
        assertThatCode(() -> index(ref, "a".repeat(MAX_TERM_LENGTH + 1)))
            .doesNotThrowAnyException();
    }
}
