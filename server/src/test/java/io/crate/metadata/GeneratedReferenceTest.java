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

import io.crate.analyze.relations.DocTableRelation;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import io.crate.types.StringType;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static io.crate.testing.T3.T1_DEFINITION;
import static io.crate.testing.T3.T1;
import static org.hamcrest.Matchers.is;

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
        GeneratedReference generatedReferenceInfo = new GeneratedReference(1,
            referenceIdent,
            RowGranularity.DOC,
            StringType.INSTANCE, ColumnPolicy.STRICT, Reference.IndexType.FULLTEXT,
            formattedGeneratedExpression,
            false,
            true);

        generatedReferenceInfo.generatedExpression(expressions.normalize(executor.asSymbol(formattedGeneratedExpression)));
        generatedReferenceInfo.referencedReferences(List.of(t1Info.getReference(new ColumnIdent("a"))));

        BytesStreamOutput out = new BytesStreamOutput();
        Reference.toStream(generatedReferenceInfo, out);

        StreamInput in = out.bytes().streamInput();
        GeneratedReference generatedReferenceInfo2 = Reference.fromStream(in);

        assertThat(generatedReferenceInfo2, is(generatedReferenceInfo));
    }
}
