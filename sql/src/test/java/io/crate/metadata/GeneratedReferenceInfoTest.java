/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import io.crate.types.StringType;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class GeneratedReferenceInfoTest extends CrateUnitTest {

    private static final SqlExpressions SQL_EXPRESSIONS = new SqlExpressions(
            MapBuilder.<QualifiedName, AnalyzedRelation>newMapBuilder()
                    .put(new QualifiedName(T3.T1_INFO.ident().fqn()), T3.TR_1)
                    .map(),
            T3.TR_1
            );

    @Test
    public void testStreaming() throws Exception {
        ReferenceIdent referenceIdent = new ReferenceIdent(T3.T1_INFO.ident(), "generated_column");
        String formattedGeneratedExpression = "concat(a, 'bar')";
        GeneratedReferenceInfo generatedReferenceInfo = new GeneratedReferenceInfo(referenceIdent, RowGranularity.DOC,
                StringType.INSTANCE, ColumnPolicy.STRICT, ReferenceInfo.IndexType.ANALYZED, formattedGeneratedExpression);

        generatedReferenceInfo.generatedExpression(SQL_EXPRESSIONS.normalize(SQL_EXPRESSIONS.asSymbol(formattedGeneratedExpression)));
        generatedReferenceInfo.referencedReferenceInfos(ImmutableList.of(T3.T1_INFO.getReferenceInfo(new ColumnIdent("a"))));

        BytesStreamOutput out = new BytesStreamOutput();
        ReferenceInfo.toStream(generatedReferenceInfo, out);

        StreamInput in = StreamInput.wrap(out.bytes());
        GeneratedReferenceInfo generatedReferenceInfo2 = ReferenceInfo.fromStream(in);

        assertThat(generatedReferenceInfo2, is(generatedReferenceInfo));
    }
}
