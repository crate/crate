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

package io.crate.expression.symbol;

import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class AggregationTest extends CrateUnitTest {

    @Test
    public void test_serialization_with_filter() throws Exception {
        Aggregation actual = new Aggregation(
            CountAggregation.COUNT_STAR_FUNCTION,
            CountAggregation.COUNT_STAR_SIGNATURE,
            CountAggregation.COUNT_STAR_FUNCTION.returnType(),
            List.of(),
            Literal.BOOLEAN_FALSE
        );
        BytesStreamOutput output = new BytesStreamOutput();
        Symbols.toStream(actual, output);

        StreamInput input = output.bytes().streamInput();
        Aggregation expected = (Aggregation) Symbols.fromStream(input);

        assertThat(expected.filter(), is(Literal.BOOLEAN_FALSE));
        assertThat(expected, is(actual));
    }

    @Test
    public void test_serialization_with_filter_before_version_4_1_0() throws Exception {
        Aggregation actual = new Aggregation(
            CountAggregation.COUNT_STAR_FUNCTION,
            CountAggregation.COUNT_STAR_SIGNATURE,
            CountAggregation.COUNT_STAR_FUNCTION.returnType(),
            List.of()
        );
        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(Version.V_4_0_0);
        Symbols.toStream(actual, output);

        StreamInput input = output.bytes().streamInput();
        input.setVersion(Version.V_4_0_0);

        Aggregation expected = (Aggregation) Symbols.fromStream(input);

        assertThat(expected.filter(), is(Literal.BOOLEAN_TRUE));
        assertThat(expected, is(actual));
    }
}
