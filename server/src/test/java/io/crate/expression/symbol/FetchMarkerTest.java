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

package io.crate.expression.symbol;

import static io.crate.testing.Asserts.assertThat;

import java.util.List;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.types.DataTypes;

public class FetchMarkerTest {

    @Test
    public void test_fetch_marker_streams_as_if_it_is_a_fetchId_reference() throws Exception {
        RelationName relationName = new RelationName("doc", "tbl");
        FetchMarker fetchMarker = new FetchMarker(relationName, List.of());

        BytesStreamOutput out = new BytesStreamOutput();
        Symbols.toStream(fetchMarker, out);

        StreamInput in = out.bytes().streamInput();
        Symbol symbol = Symbols.fromStream(in);
        assertThat(symbol)
            .isReference()
            .hasColumnIdent(ColumnIdent.of("_fetchid"))
            .hasTableIdent(relationName)
            .hasType(DataTypes.LONG);
    }
}
