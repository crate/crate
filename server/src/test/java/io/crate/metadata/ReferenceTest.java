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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class ReferenceTest extends ESTestCase {

    @Test
    public void testEquals() {
        RelationName relationName = RelationName.of("doc", "test");
        ReferenceIdent referenceIdent = new ReferenceIdent(relationName, "object_column");
        DataType<?> dataType1 = new ArrayType<>(DataTypes.UNTYPED_OBJECT);
        DataType<?> dataType2 = new ArrayType<>(DataTypes.UNTYPED_OBJECT);
        Symbol defaultExpression1 = Literal.of(Map.of("f", 10));
        Symbol defaultExpression2 = Literal.of(Map.of("f", 10));
        SimpleReference reference1 = new SimpleReference(referenceIdent,
                                                         RowGranularity.DOC,
                                                         dataType1,
                                                         1,
                                                         defaultExpression1);
        SimpleReference reference2 = new SimpleReference(referenceIdent,
                                                         RowGranularity.DOC,
                                                         dataType2,
                                                         1,
                                                         defaultExpression2);
        assertThat(reference1, is(reference2));
    }

    @Test
    public void testStreaming() throws Exception {
        RelationName relationName = RelationName.of("doc", "test");
        ReferenceIdent referenceIdent = new ReferenceIdent(relationName, "object_column");
        SimpleReference reference = new SimpleReference(
            referenceIdent,
            RowGranularity.DOC,
            new ArrayType<>(DataTypes.UNTYPED_OBJECT),
            ColumnPolicy.STRICT,
            IndexType.FULLTEXT,
            false,
            true,
            0,
            Literal.of(Map.of("f", 10)
            )
        );

        BytesStreamOutput out = new BytesStreamOutput();
        Reference.toStream(reference, out);

        StreamInput in = out.bytes().streamInput();
        Reference reference2 = Reference.fromStream(in);

        assertThat(reference2, is(reference));
    }

    @Test
    public void test_streaming_of_reference_position_before_4_6_0() throws Exception {
        RelationName relationName = RelationName.of("doc", "test");
        ReferenceIdent referenceIdent = new ReferenceIdent(relationName, "object_column");
        SimpleReference reference = new SimpleReference(
            referenceIdent,
            RowGranularity.DOC,
            new ArrayType<>(DataTypes.UNTYPED_OBJECT),
            ColumnPolicy.STRICT,
            IndexType.FULLTEXT,
            false,
            true,
            0,
            Literal.of(Map.of("f", 10)
            )
        );

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_4_5_0);
        Reference.toStream(reference, out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_4_5_0);
        Reference reference2 = Reference.fromStream(in);

        assertThat(reference2, is(reference));
    }
}
