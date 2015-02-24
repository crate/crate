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
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class OrderByTest extends CrateUnitTest {

    @Test
    public void testStreaming() throws Exception {
        ReferenceIdent nameIdent = new ReferenceIdent(new TableIdent("doc", "people"), "name");
        Reference name = new Reference(new ReferenceInfo(nameIdent, RowGranularity.DOC, DataTypes.STRING));
        OrderBy orderBy = new OrderBy(ImmutableList.<Symbol>of(name), new boolean[]{true}, new Boolean[]{true});

        BytesStreamOutput out = new BytesStreamOutput();
        orderBy.writeTo(out);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        OrderBy orderBy2 = OrderBy.fromStream(in);

        assertEquals(orderBy.orderBySymbols(), orderBy2.orderBySymbols());
        assertThat(orderBy2.reverseFlags().length, is(1));
        assertThat(orderBy2.reverseFlags()[0], is(true));
        assertThat(orderBy2.nullsFirst().length, is(1));
        assertThat(orderBy2.nullsFirst()[0], is(true));
    }
}
