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

import io.crate.analyze.symbol.InputColumn;
import io.crate.metadata.RowGranularity;
import io.crate.operation.operator.EqOperator;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.util.Collections;

public class FilterProjectionTest extends CrateUnitTest {

    @Test
    public void testStreaming() throws Exception {
        FilterProjection p = new FilterProjection(
            EqOperator.createFunction(new InputColumn(0, DataTypes.INTEGER), new InputColumn(1, DataTypes.INTEGER)),
            Collections.singletonList(new InputColumn(0))
        );
        p.requiredGranularity(RowGranularity.SHARD);

        BytesStreamOutput out = new BytesStreamOutput();
        Projection.toStream(p, out);

        StreamInput in = out.bytes().streamInput();
        FilterProjection p2 = (FilterProjection) Projection.fromStream(in);

        assertEquals(p, p2);
    }
}
