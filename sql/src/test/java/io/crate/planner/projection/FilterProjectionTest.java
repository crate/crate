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

package io.crate.planner.projection;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.RowGranularity;
import io.crate.operation.operator.AndOperator;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

public class FilterProjectionTest extends CrateUnitTest {

    @Test
    public void testStreaming() throws Exception {
        FilterProjection p = new FilterProjection();
        p.outputs(ImmutableList.<Symbol>of(new InputColumn(1)));
        p.requiredGranularity(RowGranularity.SHARD);
        p.query(TestingHelpers.createFunction(AndOperator.NAME, DataTypes.BOOLEAN, Literal.newLiteral(true), Literal.newLiteral(false)));

        BytesStreamOutput out = new BytesStreamOutput();
        Projection.toStream(p, out);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        FilterProjection p2 = (FilterProjection) Projection.fromStream(in);

        assertEquals(p, p2);
    }
}
