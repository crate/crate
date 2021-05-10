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

package io.crate.analyze;

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import org.elasticsearch.test.ESTestCase;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class OrderByTest extends ESTestCase {

    private static final RelationName TI = new RelationName("doc", "people");

    private Reference ref(String name) {
        return new Reference(new ReferenceIdent(TI, name), RowGranularity.DOC, DataTypes.STRING, 0, null);
    }

    @Test
    public void testStreaming() throws Exception {
        OrderBy orderBy = new OrderBy(List.<Symbol>of(ref("name")), new boolean[]{true}, new boolean[]{true});
        BytesStreamOutput out = new BytesStreamOutput();
        orderBy.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        OrderBy orderBy2 = new OrderBy(in);

        assertEquals(orderBy.orderBySymbols(), orderBy2.orderBySymbols());
        assertThat(orderBy2.reverseFlags().length, is(1));
        assertThat(orderBy2.reverseFlags()[0], is(true));
        assertThat(orderBy2.nullsFirst().length, is(1));
        assertThat(orderBy2.nullsFirst()[0], is(true));
    }
}
