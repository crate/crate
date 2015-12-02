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

package io.crate.analyze.symbol;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

public class ReferenceTest extends CrateUnitTest {

    private SqlExpressions sqlExpressions;
    private final TableIdent tableIdent = new TableIdent(DocSchemaInfo.NAME, "gen");
    private DocTableInfo tableInfo;

    @Before
    public void prepare() throws Exception {
        tableInfo = TestingTableInfo.builder(tableIdent,
                new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
                .add("col", DataTypes.BOOLEAN)
                .add("nested", DataTypes.OBJECT, ImmutableList.<String>of(), ColumnPolicy.STRICT)
                .add("nested", DataTypes.STRING, Collections.singletonList("name"))
                .addGeneratedColumn("gen_col", DataTypes.BOOLEAN, "not col", true)
                .addIndex(ColumnIdent.fromPath("idx"), ReferenceInfo.IndexType.ANALYZED)
                .addPartitions(new PartitionName(tableIdent, Collections.singletonList(new BytesRef("true"))).toString())
                .build();
        sqlExpressions = new SqlExpressions(ImmutableMap.<QualifiedName, AnalyzedRelation>of(QualifiedName.of("gen"), new DocTableRelation(tableInfo)));

    }

    @Test
    public void testStreaming() throws Exception {
        ReferenceInfo referenceInfo = randomFrom(ImmutableList.copyOf(tableInfo.iterator()));
        Reference ref = new Reference(referenceInfo);

        BytesStreamOutput out = new BytesStreamOutput();
        Reference.toStream(ref, out);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        Reference streamedRef = Reference.fromStream(in);

        assertThat(streamedRef.info().getClass(), equalTo((Class)ReferenceInfo.class));
        assertThat(ref.ident(), is(streamedRef.ident()));
    }
}
