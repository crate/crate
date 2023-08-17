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

package io.crate.execution.engine.aggregation.impl;

import static io.crate.testing.Asserts.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import io.crate.common.MutableLong;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.impl.templates.BinaryDocValueAggregator;
import io.crate.execution.engine.aggregation.impl.templates.SortedNumericDocValueAggregator;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SearchPath;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.sql.tree.BitString;
import io.crate.types.BitStringType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;


public class CountAggregationTest extends AggregationTestCase {

    private Object executeAggregation(DataType<?> argumentType, Object[][] data) throws Exception {
        return executeAggregation(
            CountAggregation.SIGNATURE,
            List.of(argumentType),
            DataTypes.LONG,
            data,
            true,
            List.of()
        );
    }

    private void assertHasDocValueAggregator(List<Reference> aggregationReferences,
                                             DocTableInfo sourceTable,
                                             Class<?> expectedAggregatorClass) {
        var aggregationFunction = (AggregationFunction<?, ?>) nodeCtx.functions().get(
            null,
            CountAggregation.NAME,
            InputColumn.mapToInputColumns(aggregationReferences),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        var docValueAggregator = aggregationFunction.getDocValueAggregator(
            mock(LuceneReferenceResolver.class),
            aggregationReferences,
            sourceTable,
            List.of()
        );
        if (expectedAggregatorClass == null) {
            assertThat(docValueAggregator).isNull();
        } else {
            assertThat(docValueAggregator).isNotNull();
            assertThat(docValueAggregator).isExactlyInstanceOf(expectedAggregatorClass);
        }
    }

    @Test
    public void testReturnType() {
        var countFunction = nodeCtx.functions().get(
            null,
            CountAggregation.NAME,
            List.of(Literal.of(DataTypes.INTEGER, null)),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        assertThat(countFunction.boundSignature().returnType()).isEqualTo(DataTypes.LONG);
    }

    @Test
    public void test_function_implements_doc_values_aggregator_for_numeric_types() {
        for (var dataType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            assertHasDocValueAggregator(CountAggregation.NAME, List.of(dataType));
        }
    }

    @Test
    public void test_function_implements_doc_values_aggregator_for_string_based_types() {
        for (var dataType : List.of(DataTypes.STRING, DataTypes.IP)) {
            assertHasDocValueAggregator(CountAggregation.NAME, List.of(dataType));
        }
    }

    private void helper_count_on_object_with_not_null_immediate_child(DataType<?> childType, Class<?> expectedAggregatorClass) {
        SimpleReference notNullImmediateChild = new SimpleReference(
            new ReferenceIdent(null, new ColumnIdent("top_level_object", "not_null_subcol")),
            RowGranularity.DOC,
            childType,
            IndexType.PLAIN,
            true,
            true,
            0,
            null);
        SimpleReference countedObject = new SimpleReference(
            new ReferenceIdent(null, new ColumnIdent("top_level_object")),
            RowGranularity.DOC,
            ObjectType.builder().setInnerType(notNullImmediateChild.column().leafName(), notNullImmediateChild.valueType()).build(),
            IndexType.PLAIN,
            true,
            true,
            0,
            null
        );
        DocTableInfo sourceTable = mock(DocTableInfo.class);
        when(sourceTable.notNullColumns()).thenReturn(List.of(notNullImmediateChild.column()));
        when(sourceTable.getReference(notNullImmediateChild.column())).thenReturn(notNullImmediateChild);

        assertHasDocValueAggregator(List.of(countedObject), sourceTable, expectedAggregatorClass);
        verify(sourceTable, times(1)).notNullColumns();
        verify(sourceTable, times(1)).getReference(notNullImmediateChild.column());
    }

    @Test
    public void test_count_on_object_with_immediate_not_null_subcolumn_uses_DocValueAggregator() {
        var countedObjectToExpectedAggregatorMap = Map.ofEntries(
            Map.entry(DataTypes.BYTE, SortedNumericDocValueAggregator.class),
            Map.entry(DataTypes.TIMESTAMPZ, SortedNumericDocValueAggregator.class),
            Map.entry(DataTypes.GEO_POINT, SortedNumericDocValueAggregator.class),
            Map.entry(DataTypes.IP, BinaryDocValueAggregator.class),
            Map.entry(DataTypes.STRING, BinaryDocValueAggregator.class)
        );
        for (var e : countedObjectToExpectedAggregatorMap.entrySet()) {
            helper_count_on_object_with_not_null_immediate_child(e.getKey(), e.getValue());
        }
    }

    @Test
    public void test_count_on_object_with_deeper_not_null_subcolumn_uses_DocValueAggregator() {
        SimpleReference notNullGrandChild = new SimpleReference(
            new ReferenceIdent(
                null,
                new ColumnIdent("top_level_object", List.of("second_level_object", "not_null_subcol"))),
            RowGranularity.DOC,
            DataTypes.STRING,
            IndexType.PLAIN,
            true,
            true,
            0,
            null);
        SimpleReference immediateChild = new SimpleReference(
            new ReferenceIdent(null, new ColumnIdent("top_level_object", "second_level_object")),
            RowGranularity.DOC,
            ObjectType.builder().setInnerType(notNullGrandChild.column().leafName(), notNullGrandChild.valueType()).build(),
            IndexType.PLAIN,
            true,
            true,
            0,
            null
        );
        SimpleReference countedObject = new SimpleReference(
            new ReferenceIdent(null, new ColumnIdent("top_level_object")),
            RowGranularity.DOC,
            ObjectType.builder().setInnerType(immediateChild.column().leafName(), immediateChild.valueType()).build(),
            IndexType.PLAIN,
            true,
            true,
            0,
            null
        );
        DocTableInfo sourceTable = mock(DocTableInfo.class);
        when(sourceTable.notNullColumns()).thenReturn(List.of(notNullGrandChild.column()));
        when(sourceTable.getReference(notNullGrandChild.column())).thenReturn(notNullGrandChild);

        assertHasDocValueAggregator(List.of(countedObject), sourceTable, BinaryDocValueAggregator.class);
        verify(sourceTable, times(1)).notNullColumns();
        verify(sourceTable, times(1)).getReference(notNullGrandChild.column());
    }

    @Test
    public void test_count_on_object_with_not_null_sibling_not_use_DocValueAggregator() {
        SimpleReference notNullSibling = new SimpleReference(
            new ReferenceIdent(
                null,
                new ColumnIdent("top_level_Integer")),
            RowGranularity.DOC,
            DataTypes.INTEGER,
            0,
            null);
        SimpleReference countedObject = new SimpleReference(
            new ReferenceIdent(null, new ColumnIdent("top_level_object")),
            RowGranularity.DOC,
            ObjectType.DYNAMIC_OBJECT,
            0,
            null
        );
        DocTableInfo sourceTable = mock(DocTableInfo.class);
        when(sourceTable.notNullColumns()).thenReturn(List.of(notNullSibling.column()));
        when(sourceTable.getReference(notNullSibling.column())).thenReturn(notNullSibling);

        assertHasDocValueAggregator(List.of(countedObject), sourceTable, null);
        verify(sourceTable, times(1)).notNullColumns();
        verify(sourceTable, times(0)).getReference(notNullSibling.column());
    }

    @Test
    public void test_count_on_object_with_not_null_siblings_child_not_use_DocValueAggregator() {
        SimpleReference notNullSibilingsChild = new SimpleReference(
            new ReferenceIdent(
                null,
                new ColumnIdent("top_level_sibling", List.of("not_null_subcol"))),
            RowGranularity.DOC,
            DataTypes.STRING,
            0,
            null);
        @SuppressWarnings("unused")
        SimpleReference sibling = new SimpleReference(
            new ReferenceIdent(null, new ColumnIdent("top_level_sibling")),
            RowGranularity.DOC,
            ObjectType.builder().setInnerType(notNullSibilingsChild.column().leafName(), notNullSibilingsChild.valueType()).build(),
            0,
            null
        );
        SimpleReference countedObject = new SimpleReference(
            new ReferenceIdent(null, new ColumnIdent("top_level_object")),
            RowGranularity.DOC,
            ObjectType.DYNAMIC_OBJECT,
            0,
            null
        );
        DocTableInfo sourceTable = mock(DocTableInfo.class);
        when(sourceTable.notNullColumns()).thenReturn(List.of(notNullSibilingsChild.column()));
        when(sourceTable.getReference(notNullSibilingsChild.column())).thenReturn(notNullSibilingsChild);

        assertHasDocValueAggregator(List.of(countedObject), sourceTable, null);
        verify(sourceTable, times(1)).notNullColumns();
        verify(sourceTable, times(0)).getReference(notNullSibilingsChild.column());
    }

    @Test
    public void test_count_on_object_with_nullable_subcolumn_not_use_DocValueAggregator() {
        SimpleReference nullableChild = new SimpleReference(
            new ReferenceIdent(null, new ColumnIdent("top_level_object", "nullable_subcol")),
            RowGranularity.DOC,
            DataTypes.INTEGER,
            0,
            null);
        SimpleReference countedObject = new SimpleReference(
            new ReferenceIdent(null, new ColumnIdent("top_level_object")),
            RowGranularity.DOC,
            ObjectType.builder().setInnerType(nullableChild.column().leafName(), nullableChild.valueType()).build(),
            0,
            null
        );
        DocTableInfo sourceTable = mock(DocTableInfo.class);
        when(sourceTable.notNullColumns()).thenReturn(List.of());
        when(sourceTable.getReference(nullableChild.column())).thenReturn(nullableChild);

        assertHasDocValueAggregator(List.of(countedObject), sourceTable, null);
        verify(sourceTable, times(1)).notNullColumns();
        verify(sourceTable, times(0)).getReference(nullableChild.column());
    }

    @Test
    public void test_count_on_object_with_multiple_not_null_candidates() {
        SimpleReference notNullGrandChild1 = new SimpleReference(
            new ReferenceIdent(
                null,
                new ColumnIdent("top_level_object", List.of("second_level_object", "not_null_subcol1"))),
            RowGranularity.DOC,
            DataTypes.STRING,
            IndexType.PLAIN,
            true,
            true,
            0,
            null);
        SimpleReference notNullGrandChild2 = new SimpleReference(
            new ReferenceIdent(
                null,
                new ColumnIdent("top_level_object", List.of("second_level_object", "not_null_subcol2"))),
            RowGranularity.DOC,
            DataTypes.BYTE,
            IndexType.PLAIN,
            true,
            true,
            0,
            null);
        SimpleReference immediateChild = new SimpleReference(
            new ReferenceIdent(null, new ColumnIdent("top_level_object", "second_level_object")),
            RowGranularity.DOC,
            ObjectType.builder()
                .setInnerType(notNullGrandChild1.column().leafName(), notNullGrandChild1.valueType())
                .setInnerType(notNullGrandChild2.column().leafName(), notNullGrandChild2.valueType())
                .build(),
            IndexType.PLAIN,
            true,
            true,
            0,
            null
        );
        SimpleReference notNullImmediateChild = new SimpleReference(
            new ReferenceIdent(null, new ColumnIdent("top_level_object", "not_null_subcol")),
            RowGranularity.DOC,
            DataTypes.IP,
            IndexType.PLAIN,
            true,
            true,
            0,
            null);
        SimpleReference countedObject = new SimpleReference(
            new ReferenceIdent(null, new ColumnIdent("top_level_object")),
            RowGranularity.DOC,
            ObjectType.builder()
                .setInnerType(immediateChild.column().leafName(), immediateChild.valueType())
                .setInnerType(notNullImmediateChild.column().leafName(), notNullImmediateChild.valueType())
                .build(),
            IndexType.PLAIN,
            true,
            true,
            0,
            null
        );
        DocTableInfo sourceTable = mock(DocTableInfo.class);
        when(sourceTable.notNullColumns()).thenReturn(List.of(notNullGrandChild1.column(),
                                                              notNullGrandChild2.column(),
                                                              notNullImmediateChild.column()));
        when(sourceTable.getReference(notNullGrandChild1.column())).thenReturn(notNullGrandChild1);
        when(sourceTable.getReference(notNullGrandChild2.column())).thenReturn(notNullGrandChild2);
        when(sourceTable.getReference(notNullImmediateChild.column())).thenReturn(notNullImmediateChild);
        assertHasDocValueAggregator(List.of(countedObject), sourceTable, BinaryDocValueAggregator.class);
        verify(sourceTable, times(1)).notNullColumns();
        verify(sourceTable, times(1)).getReference(notNullGrandChild1.column());
    }

    @Test
    public void testDouble() throws Exception {
        assertThat(executeAggregation(DataTypes.DOUBLE, new Object[][]{{0.7d}, {0.3d}})).isEqualTo(2L);
    }

    @Test
    public void testFloat() throws Exception {
        assertThat(executeAggregation(DataTypes.FLOAT, new Object[][]{{0.7f}, {0.3f}})).isEqualTo(2L);
    }

    @Test
    public void testInteger() throws Exception {
        assertThat(executeAggregation(DataTypes.INTEGER, new Object[][]{{7}, {3}})).isEqualTo(2L);
    }

    @Test
    public void testLong() throws Exception {
        assertThat(executeAggregation(DataTypes.LONG, new Object[][]{{7L}, {3L}})).isEqualTo(2L);
    }

    @Test
    public void testShort() throws Exception {
        assertThat(executeAggregation(DataTypes.SHORT, new Object[][]{{(short) 7}, {(short) 3}})).isEqualTo(2L);
    }

    @Test
    public void testString() throws Exception {
        assertThat(executeAggregation(DataTypes.STRING, new Object[][]{{"Youri"}, {"Ruben"}})).isEqualTo(2L);
    }

    @Test
    public void test_count_with_ip_argument() throws Exception {
        assertThat(executeAggregation(DataTypes.IP, new Object[][]{{"127.0.0.1"}})).isEqualTo(1L);
    }

    @Test
    public void test_count_with_bitstring_argument() throws Exception {
        BitStringType bitStringType = new BitStringType(4);
        Object[][] rows = new Object[][] {
            { BitString.ofRawBits("0100") },
            { null },
            { BitString.ofRawBits("0110") },
        };
        assertThat(executeAggregation(bitStringType, rows)).isEqualTo(2L);
        assertHasDocValueAggregator("count", List.of(bitStringType));
    }

    @Test
    public void test_count_with_geo_point_argument() throws Exception {
        assertThat(executeAggregation(DataTypes.GEO_POINT, new Object[][]{{new double[]{1, 2}}})).isEqualTo(1L);
    }

    @Test
    public void testNormalizeWithNullLiteral() {
        assertThat(normalize("count", null, DataTypes.STRING)).isLiteral(0L);
        assertThat(normalize("count", null, DataTypes.UNDEFINED)). isLiteral(0L);
    }

    @Test
    public void test_count_star() throws Exception {
        assertThat(executeAggregation(CountAggregation.COUNT_STAR_SIGNATURE, new Object[][]{{}, {}}, List.of())).isEqualTo(2L);
    }

    @Test
    public void testStreaming() throws Exception {
        MutableLong l1 = new MutableLong(12345L);
        BytesStreamOutput out = new BytesStreamOutput();
        var streamer = CountAggregation.LongStateType.INSTANCE.streamer();
        streamer.writeValueTo(out, l1);
        StreamInput in = out.bytes().streamInput();
        MutableLong l2 = streamer.readValueFrom(in);
        assertThat(l1.value()).isEqualTo(l2.value());
    }
}
