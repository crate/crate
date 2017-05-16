/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.AmbiguousOrderByException;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.util.ArrayList;

import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.is;

public class OrderByTest extends CrateUnitTest {

    private static final TableIdent TI = new TableIdent("doc", "people");

    private Reference ref(String name) {
        return new Reference(new ReferenceIdent(TI, name), RowGranularity.DOC, DataTypes.STRING);
    }

    @Test
    public void testStreaming() throws Exception {
        OrderBy orderBy = new OrderBy(ImmutableList.<Symbol>of(ref("name")), new boolean[]{true}, new Boolean[]{true});
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

    @Test
    public void testSubset() throws Exception {
        OrderBy orderBy = new OrderBy(ImmutableList.<Symbol>of(
            ref("a"), ref("b"), ref("c")), new boolean[]{true, false, true}, new Boolean[]{true, null, false});
        assertThat(orderBy.subset(ImmutableList.of(0, 2)), isSQL("doc.people.a DESC NULLS FIRST, doc.people.c DESC NULLS LAST"));
        assertThat(orderBy.subset(ImmutableList.of(1)), isSQL("doc.people.b"));
    }

    @Test
    public void testMerge() throws Exception {
        OrderBy orderBy1 = new OrderBy(new ArrayList<Symbol>() {{add(ref("a")); add(ref("b")); add(ref("c"));}},
            new boolean[]{true, false, true}, new Boolean[]{true, null, false});
        OrderBy orderBy2 = new OrderBy(new ArrayList<Symbol>() {{add(ref("b")); add(ref("c")); add(ref("d"));}},
            new boolean[]{false, true, false}, new Boolean[]{null, false, null});

        assertThat(orderBy1.merge(orderBy2),
            isSQL("doc.people.b, doc.people.c DESC NULLS LAST, doc.people.d, doc.people.a DESC NULLS FIRST"));
    }

    @Test
    public void testMergeAmbiguous() throws Exception {
        OrderBy orderBy1 = new OrderBy(new ArrayList<Symbol>() {{add(ref("a"));}},
            new boolean[]{true}, new Boolean[]{true});
        OrderBy orderBy2 = new OrderBy(new ArrayList<Symbol>() {{add(ref("a"));}},
            new boolean[]{false}, new Boolean[]{true});

        expectedException.expect(AmbiguousOrderByException.class);
        expectedException.expectMessage("is ambiguous");
        orderBy1.merge(orderBy2);
    }
}
