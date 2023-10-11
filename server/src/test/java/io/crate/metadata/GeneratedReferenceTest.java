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

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.T3.T1;
import static io.crate.testing.T3.T1_DEFINITION;

import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.DocTableRelation;
import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataTypes;
import io.crate.types.StringType;

public class GeneratedReferenceTest extends CrateDummyClusterServiceUnitTest {

    SQLExecutor executor;
    private SqlExpressions expressions;
    private DocTableInfo t1Info;

    @Before
    public void prepare() throws Exception {
        executor = SQLExecutor.builder(clusterService)
            .addTable(T1_DEFINITION)
            .build();
        t1Info = executor.schemas().getTableInfo(T1);

        DocTableRelation tableRelation = new DocTableRelation(t1Info);
        tableRelation.getField(new ColumnIdent("a"));   // allocate field so it can be resolved
        expressions = new SqlExpressions(Collections.emptyMap(), tableRelation);
    }

    @Test
    public void testStreaming() throws Exception {
        ReferenceIdent referenceIdent = new ReferenceIdent(t1Info.ident(), "generated_column");
        String formattedGeneratedExpression = "concat(a, 'bar')";
        SimpleReference simpleRef = new SimpleReference(referenceIdent, RowGranularity.DOC, StringType.INSTANCE, 1, null);
        Symbol generatedExpression = expressions.normalize(executor.asSymbol(formattedGeneratedExpression));
        GeneratedReference generatedReferenceInfo = new GeneratedReference(simpleRef, formattedGeneratedExpression, generatedExpression);
        generatedReferenceInfo.referencedReferences(List.of(t1Info.getReference(new ColumnIdent("a"))));

        BytesStreamOutput out = new BytesStreamOutput();
        Reference.toStream(out, generatedReferenceInfo);

        StreamInput in = out.bytes().streamInput();
        GeneratedReference generatedReferenceInfo2 = Reference.fromStream(in);

        assertThat(generatedReferenceInfo2).isEqualTo(generatedReferenceInfo);
    }

    @Test
    public void test_streaming_generated_reference_with_null_expression_symbol() throws Exception {
        ReferenceIdent referenceIdent = new ReferenceIdent(t1Info.ident(), "generated_column");
        String formattedGeneratedExpression = "concat(a, 'bar')";
        SimpleReference simpleRef = new SimpleReference(referenceIdent, RowGranularity.DOC, StringType.INSTANCE, 1, null);
        GeneratedReference generatedReference = new GeneratedReference(simpleRef, formattedGeneratedExpression, null);

        BytesStreamOutput out = new BytesStreamOutput();
        Reference.toStream(out, generatedReference);

        StreamInput in = out.bytes().streamInput();
        GeneratedReference generatedReference2 = Reference.fromStream(in);

        assertThat(generatedReference2).isEqualTo(generatedReference);

    }

    @Test
    public void test_generated_reference_cast_keeps_generated_reference() throws Exception {
        var relationName = new RelationName("doc", "tbl");
        var referenceIdent = new ReferenceIdent(relationName, "year");
        var simpleRef = new SimpleReference(
            referenceIdent,
            RowGranularity.DOC,
            DataTypes.LONG,
            1,
            null
        );
        var generatedReference = new GeneratedReference(
            simpleRef,
            "date_trunc('year', ts)",
            null
        );
        Symbol cast = generatedReference.cast(DataTypes.STRING, CastMode.EXPLICIT);
        assertThat(cast).isFunction(
            "cast",
            arg1 -> {
                assertThat(arg1).isExactlyInstanceOf(GeneratedReference.class);
                assertThat(arg1).isReference().hasName("year");
            },
            arg2 -> assertThat(arg2).isLiteral(null)
        );
    }
}
