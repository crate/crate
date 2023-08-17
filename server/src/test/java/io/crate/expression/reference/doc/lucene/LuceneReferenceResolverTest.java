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

package io.crate.expression.reference.doc.lucene;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.junit.Test;

import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.Function;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class LuceneReferenceResolverTest extends CrateDummyClusterServiceUnitTest {

    // just return any fieldType to get passt the null check
    private static final RelationName RELATION_NAME = new RelationName("s", "t");
    private static final LuceneReferenceResolver LUCENE_REFERENCE_RESOLVER = new LuceneReferenceResolver(
        RELATION_NAME.indexNameOrAlias(),
        i -> new KeywordFieldType("dummy", true, false),
        List.of()
    );

    @Test
    public void testGetImplementationWithColumnsOfTypeCollection() {
        SimpleReference arrayRef = new SimpleReference(
            new ReferenceIdent(RELATION_NAME, "a"), RowGranularity.DOC, DataTypes.DOUBLE_ARRAY, 0, null
        );
        assertThat(LUCENE_REFERENCE_RESOLVER.getImplementation(arrayRef))
            .isExactlyInstanceOf(DocCollectorExpression.ChildDocCollectorExpression.class);
    }

    @Test
    public void testGetImplementationForSequenceNumber() {
        SimpleReference seqNumberRef = new SimpleReference(
            new ReferenceIdent(RELATION_NAME, "_seq_no"), RowGranularity.DOC, DataTypes.LONG, 0, null
        );
        assertThat(LUCENE_REFERENCE_RESOLVER.getImplementation(seqNumberRef))
            .isExactlyInstanceOf(SeqNoCollectorExpression.class);
    }

    @Test
    public void testGetImplementationForPrimaryTerm() {
        SimpleReference primaryTerm = new SimpleReference(
            new ReferenceIdent(RELATION_NAME, "_primary_term"), RowGranularity.DOC, DataTypes.LONG, 0, null
        );
        assertThat(LUCENE_REFERENCE_RESOLVER.getImplementation(primaryTerm))
            .isExactlyInstanceOf(PrimaryTermCollectorExpression.class);
    }

    @Test
    public void test_ignored_dynamic_references_are_resolved_using_sourcelookup() {
        Reference ignored = new DynamicReference(
                new ReferenceIdent(RELATION_NAME, "a", List.of("b")), RowGranularity.DOC, 0, ColumnPolicy.DYNAMIC);

        assertThat(LUCENE_REFERENCE_RESOLVER.getImplementation(ignored))
            .isExactlyInstanceOf(DocCollectorExpression.ChildDocCollectorExpression.class);
    }

    @Test
    public void test_can_lookup_generated_partition_column_if_casted() throws Exception {
        // See https://github.com/crate/crate/issues/14307
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addPartitionedTable("""
                create table tbl (
                    ts timestamp,
                    year as date_trunc('year', ts)
                ) partitioned by (year)
                """
            )
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        table.partitionedByColumns();
        PartitionName partitionName = new PartitionName(new RelationName("doc", "tbl"), List.of("2023"));
        LuceneReferenceResolver refResolver = new LuceneReferenceResolver(
            partitionName.asIndexName(),
            i -> new KeywordFieldType("dummy", true, false),
            table.partitionedByColumns()
        );
        Reference year = table.getReference(new ColumnIdent("year"));
        LuceneCollectorExpression<?> impl1 = refResolver.getImplementation(year);
        assertThat(impl1).isExactlyInstanceOf(LuceneReferenceResolver.LiteralValueExpression.class);
        assertThat(impl1.value()).isEqualTo(2023L);

        Function cast = (Function) year.cast(DataTypes.STRING, CastMode.EXPLICIT);
        Reference castYearRef = (Reference) cast.arguments().get(0);
        LuceneCollectorExpression<?> impl2 = refResolver.getImplementation(castYearRef);
        assertThat(impl2).isExactlyInstanceOf(LuceneReferenceResolver.LiteralValueExpression.class);
        assertThat(impl2.value()).isEqualTo(2023L);

        SimpleReference yearSimpleRef = new SimpleReference(
            year.ident(),
            RowGranularity.PARTITION,
            year.valueType(),
            year.position(),
            year.defaultExpression()
        );
        LuceneCollectorExpression<?> impl3 = refResolver.getImplementation(yearSimpleRef);
        assertThat(impl3).isExactlyInstanceOf(LuceneReferenceResolver.LiteralValueExpression.class);
        assertThat(impl3.value()).isEqualTo(2023L);
    }
}
