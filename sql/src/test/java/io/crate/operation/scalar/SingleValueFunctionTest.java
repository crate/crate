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

package io.crate.operation.scalar;

import io.crate.analyze.symbol.Literal;
import io.crate.types.LongType;
import io.crate.types.SingleColumnTableType;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

public class SingleValueFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testEvaluate() {
        assertEvaluate(
            "single_value(table_long)",
            1L,
            Literal.of(new SingleColumnTableType(LongType.INSTANCE), new Long[]{1L}));

        assertEvaluate(
            "single_value(table_string)",
            "test",
            Literal.of(new SingleColumnTableType(LongType.INSTANCE), new BytesRef[]{new BytesRef("test")}));

        assertEvaluate(
            "single_value(table_string)",
            null,
            Literal.of(new SingleColumnTableType(LongType.INSTANCE), null));
    }

    @Test
    public void testMoreThanOneValue() {
        expectedException.expect(UnsupportedOperationException.class);
        assertEvaluate(
            "single_value(table_long)",
            1L,
            Literal.of(new SingleColumnTableType(LongType.INSTANCE), new Long[]{1L, 2L}));
    }
}
