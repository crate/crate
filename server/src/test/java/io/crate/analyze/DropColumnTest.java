/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import static io.crate.analyze.AnalyzedAlterTableDropColumn.DropColumn;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.expression.symbol.Literal;
import io.crate.metadata.IndexType;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

public class DropColumnTest extends ESTestCase {

    @Test
    public void testStreaming() throws Exception {
        RelationName relationName = new RelationName("doc", "test");
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
            111,
            true,
            Literal.of(Map.of("f", 10)
            )
        );
        boolean ifExists = randomBoolean();
        DropColumn dropColumn = new DropColumn(reference, ifExists);

        BytesStreamOutput out = new BytesStreamOutput();
        DropColumn.toStream(out, dropColumn);

        StreamInput in = out.bytes().streamInput();
        DropColumn dropColumn2 = DropColumn.fromStream(in);

        assertThat(dropColumn2).isEqualTo(dropColumn);
    }

}
