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
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RowGranularity;
import io.crate.operation.operator.AndOperator;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.util.HashMap;

public class WriterProjectionTest extends CrateUnitTest {

    @Test
    public void testStreaming() throws Exception {
        WriterProjection p = new WriterProjection(
                ImmutableList.<Symbol>of(new InputColumn(1)),
                Literal.newLiteral("/foo.json"),
                false,
                WriterProjection.CompressionType.GZIP,
                MapBuilder.<ColumnIdent, Symbol>newMapBuilder().put(
                        new ColumnIdent("partitionColumn"), Literal.newLiteral(1)).map(),
                ImmutableList.of("foo"),
                WriterProjection.OutputFormat.JSON_OBJECT
        );

        BytesStreamOutput out = new BytesStreamOutput();
        Projection.toStream(p, out);

        StreamInput in = StreamInput.wrap(out.bytes());
        WriterProjection p2 = (WriterProjection) Projection.fromStream(in);

        assertEquals(p, p2);
    }
}
