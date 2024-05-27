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

package io.crate.execution.dsl.projection;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.common.collections.MapBuilder;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;

public class WriterProjectionTest extends ESTestCase {

    @Test
    public void testStreaming() throws Exception {
        WriterProjection p = new WriterProjection(
            List.of(new InputColumn(1)),
            Literal.of("/foo.json"),
            WriterProjection.CompressionType.GZIP,
            MapBuilder.<ColumnIdent, Symbol>newMapBuilder().put(
                new ColumnIdent("partitionColumn"), Literal.of(1)).map(),
            List.of("foo"),
            WriterProjection.OutputFormat.JSON_OBJECT,
            Settings.builder().put("protocol", "http").build()
        );

        BytesStreamOutput out = new BytesStreamOutput();
        Projection.toStream(p, out);

        StreamInput in = out.bytes().streamInput();
        WriterProjection p2 = (WriterProjection) Projection.fromStream(in);

        assertThat(p2).isEqualTo(p);
    }


    @Test
    public void testStreamingBefore4_8_0() throws Exception {
        WriterProjection actualInput = new WriterProjection(
            List.of(new InputColumn(1)),
            Literal.of("/foo.json"),
            WriterProjection.CompressionType.GZIP,
            MapBuilder.<ColumnIdent, Symbol>newMapBuilder().put(
                new ColumnIdent("partitionColumn"), Literal.of(1)).map(),
            List.of("foo"),
            WriterProjection.OutputFormat.JSON_OBJECT,
            Settings.builder().put("protocol", "dummyHTTPS").build()
        );

        WriterProjection expected = new WriterProjection(
            List.of(new InputColumn(1)),
            Literal.of("/foo.json"),
            WriterProjection.CompressionType.GZIP,
            MapBuilder.<ColumnIdent, Symbol>newMapBuilder().put(
                new ColumnIdent("partitionColumn"), Literal.of(1)).map(),
            List.of("foo"),
            WriterProjection.OutputFormat.JSON_OBJECT,
            Settings.EMPTY
        );

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_4_7_0);
        Projection.toStream(actualInput, out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_4_7_0);
        WriterProjection actual = (WriterProjection) Projection.fromStream(in);

        assertThat(actual).isEqualTo(expected);
    }
}
