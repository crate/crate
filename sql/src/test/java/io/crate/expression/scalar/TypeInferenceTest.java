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

package io.crate.expression.scalar;

import io.crate.exceptions.ConversionException;
import org.junit.Test;

/**
 * Additional tests to {@link io.crate.metadata.functions.params.FuncParamsTest}
 */
public class TypeInferenceTest extends AbstractScalarFunctionsTest {

    @Test
    public void testComparison() {
        assertEvaluate("1 = 1", true);
        assertEvaluate("1::integer = 1::long", true);
        assertEvaluate("1.0 = 1", true);
        assertEvaluate("1.0::float = 1.0::double", true);
        assertEvaluate("1.0::float = 1.2::double", false);
        assertEvaluate("'1' = 1", true);

        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast 'foo' to type bigint");
        assertEvaluate("'foo' = 1", true);
    }

    @Test
    public void testCoalesce() throws Exception {
        assertEvaluate("coalesce(null, 1::integer, 2::long, 3.0::double, 2.5::float)", 1.0);
        assertEvaluate("coalesce(null, 1::integer, 2::long, 2.5::float)", 1.0f);
        assertEvaluate("coalesce(null, 1::integer, 2::long)", 1L);
        assertEvaluate("coalesce(null, 1::integer, '1')", 1);
        assertEvaluate("coalesce(null, 1::integer)", 1);
        assertEvaluate("coalesce(null)", null);
    }

    @Test
    public void testTimestampCalculationsWithNumerics() throws Exception {
        assertEvaluate("current_timestamp between current_timestamp - 10 and current_timestamp + 10", true);
    }

    @Test
    public void testCase() throws Exception {
        assertEvaluate("case 1 when 1 then 'foo' else 'bar' end", "foo");
        assertEvaluate("case 1 when 1.1 then 'foo' else 'bar' end", "bar");
        assertEvaluate("case 1 when 1.0 then 'foo' else 'bar' end", "foo");

        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast 'foo' to type bigint");
        assertEvaluate("case 1 when 'foo' then 'foo' else 'bar' end", "bar");
    }

    @Test
    public void testIn() {
        assertEvaluate("1 in (null, 1::integer, 2::long, 3.0, '1')", true);
        assertEvaluate("1.0 in (null, 1::integer, 2::long, 3.0)", true);
        assertEvaluate("1.2 in (1::integer, 2::long, 3.0)", false);
        assertEvaluate("1.2 in (null, 1::integer, 2::long, 3.0)", null);

        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast 'foo' to type double precision");
        assertEvaluate("1 in (null, 1::integer, 2::long, 3.0, 'foo')", true);
    }

    @Test
    public void testAny() {
        assertEvaluate("1 = ANY ([null, 1::integer, 2::long, 3.0, '1'])", true);
        assertEvaluate("1.0 = ANY ([null, 1::integer, 2::long, 3.0])", true);
        assertEvaluate("1.0 = ANY ([1::integer, 2::long, 3.0])", true);
        assertEvaluate("1.2 = ANY ([null, 1::integer, 2::long, 3.0])", null);

        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast 'foo' to type double precision");
        assertEvaluate("1 = ANY ([null, 1::integer, 2::long, 3.0, 'foo'])", true);
    }

    /**
     * Tests that timestamp operations interpret the timestamp as long (legacy feature).
     */
    @Test
    public void testTimestampOperations() {
        assertEvaluate("3::timestamp with time zone - 1", 2L);
        assertEvaluate("3000::timestamp with time zone / 1000", 3L);
        assertEvaluate("3000::timestamp with time zone/ 1000.0", 3.0);
    }
}
