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

import io.crate.metadata.table.ColumnPolicy;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class ReferenceTest extends CrateUnitTest {

    @Test
    public void testEquals() throws Exception {
        RelationName relationName = new RelationName("doc", "test");
        ReferenceIdent referenceIdent = new ReferenceIdent(relationName, "object_column");
        DataType dataType1 = new ArrayType(DataTypes.OBJECT);
        DataType dataType2 = new ArrayType(DataTypes.OBJECT);
        Reference reference1 = new Reference(referenceIdent, RowGranularity.DOC, dataType1);
        Reference reference2 = new Reference(referenceIdent, RowGranularity.DOC, dataType2);
        assertTrue(reference1.equals(reference2));
    }

    @Test
    public void testStreaming() throws Exception {
        RelationName relationName = new RelationName("doc", "test");
        ReferenceIdent referenceIdent = new ReferenceIdent(relationName, "object_column");
        Reference reference = new Reference(referenceIdent,
            RowGranularity.DOC,
            new ArrayType(DataTypes.OBJECT),
            ColumnPolicy.STRICT,
            Reference.IndexType.ANALYZED, false);

        BytesStreamOutput out = new BytesStreamOutput();
        Reference.toStream(reference, out);

        StreamInput in = out.bytes().streamInput();
        Reference reference2 = Reference.fromStream(in);

        assertThat(reference2, is(reference));
    }
}
