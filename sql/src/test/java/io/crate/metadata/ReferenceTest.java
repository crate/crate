/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.Expression;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.IntegerType;
import io.crate.types.ObjectType;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class ReferenceTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor executor;
    private TableInfo tableInfo;

    @Before
    public void prepare() throws Exception {
        executor = SQLExecutor.builder(clusterService)
            .addTable(
                "create table doc.test (" +
                "  int_column integer" +
                ")"
            )
            .build();
        tableInfo = executor.schemas().getTableInfo(new RelationName("doc", "test"));

    }

    @Test
    public void testEquals()  {
        ReferenceIdent referenceIdent = new ReferenceIdent(tableInfo.ident(), "object_column");
        DataType dataType1 = new ArrayType(ObjectType.untyped());
        DataType dataType2 = new ArrayType(ObjectType.untyped());
        Reference reference1 = new Reference(referenceIdent, RowGranularity.DOC, dataType1, null, null);
        Reference reference2 = new Reference(referenceIdent, RowGranularity.DOC, dataType2, null, null);
        assertThat(reference1, is(reference2));
    }

    @Test
    public void testStreaming() throws Exception {
        ReferenceIdent referenceIdent = new ReferenceIdent(tableInfo.ident(), "object_column");
        Reference reference = new Reference(
            referenceIdent,
            RowGranularity.DOC,
            new ArrayType(ObjectType.untyped()),
            ColumnPolicy.STRICT,
            Reference.IndexType.ANALYZED,
            false,
            null,
            null
        );

        BytesStreamOutput out = new BytesStreamOutput();
        Reference.toStream(reference, out);

        StreamInput in = out.bytes().streamInput();
        Reference reference2 = Reference.fromStream(in);

        assertThat(reference2, is(reference));
    }

    @Test
    public void testStreamingWithDefaultExpression() throws Exception {
        ReferenceIdent referenceIdent = new ReferenceIdent(tableInfo.ident(), "int_column");
        ExpressionAnalyzer analyzer
            = new ExpressionAnalyzer(executor.functions(),
                                     CoordinatorTxnCtx.systemTransactionContext(),
                                     ParamTypeHints.EMPTY,
                                     FieldProvider.UNSUPPORTED,
                                     null);
        Expression expression = SqlParser.createExpression("1+1");
        Symbol defaultExpression = analyzer.convert(expression, new ExpressionAnalysisContext());

        Reference reference = new Reference(
            referenceIdent,
            RowGranularity.DOC,
            IntegerType.INSTANCE,
            ColumnPolicy.STRICT,
            Reference.IndexType.ANALYZED,
            false,
            null,
            defaultExpression
        );

        BytesStreamOutput out = new BytesStreamOutput();
        Reference.toStream(reference, out);

        StreamInput in = out.bytes().streamInput();
        Reference reference2 = Reference.fromStream(in);

        assertThat(reference2, is(reference));
    }
}
