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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;

public class StringIndexerTest {

    @Test
    public void test_too_large_doc_value_error_contains_column_name() {
        Reference ref = mock(Reference.class);
        when(ref.storageIdent()).thenReturn("356");
        when(ref.column()).thenReturn(new ColumnIdent("network"));
        when(ref.indexType()).thenReturn(IndexType.NONE);
        when(ref.hasDocValues()).thenReturn(true);

        String value = "a".repeat(32767);

        assertThatThrownBy(() -> new StringIndexer(ref).indexValue(value, null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("DocValuesField \"network\" is too large, must be <= 32766");
    }
}
