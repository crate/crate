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

package io.crate.execution.engine.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static io.crate.testing.TestingHelpers.createReference;

import org.apache.lucene.search.SortedSetSortField;
import org.elasticsearch.index.fielddata.NullValueOrder;
import org.junit.Test;

import io.crate.expression.reference.doc.lucene.NullSentinelValues;
import io.crate.metadata.ColumnIdent;
import io.crate.types.CharacterType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class SortSymbolVisitorTest {

    @Test
    public void test_missing_object_is_implemented_for_all_primitives() {
        for (DataType<?> primitiveType : DataTypes.PRIMITIVE_TYPES) {
            NullSentinelValues.nullSentinel(primitiveType, NullValueOrder.FIRST, false);
        }
    }

    @Test
    public void test_character_type_reference_can_be_mapped_to_sort_field() {
        var ref = createReference("c", ColumnIdent.fromPath("c"), CharacterType.INSTANCE);
        var sortField = SortSymbolVisitor.mappedSortField(ref, false, NullValueOrder.FIRST);
        assertThat(sortField).isExactlyInstanceOf(SortedSetSortField.class);
    }
}
