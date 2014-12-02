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

package io.crate.operation.projectors;

import com.google.common.collect.ImmutableSet;
import io.crate.metadata.ColumnIdent;
import io.crate.operation.collect.CollectExpression;
import io.crate.testing.TestingHelpers;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class WriterProjectorTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testWriteRawToFile() throws Exception {

        String uri = folder.newFile("out.json").getAbsolutePath();
        Settings settings = ImmutableSettings.EMPTY;
        WriterProjector projector = new WriterProjector(
                uri,
                settings,
                null,
                ImmutableSet.<CollectExpression<?>>of(),
                new HashMap<ColumnIdent, Object>()
        );
        Projector downstream = new CollectingProjector();
        projector.downstream(downstream);

        projector.startProjection();

        projector.registerUpstream(null);
        for (int i = 0; i < 5; i++) {
            projector.setNextRow(new BytesRef(String.format("input line %02d", i)));
        }
        projector.upstreamFinished();

        Object[][] rows = ((ResultProvider) downstream).result().get();

        assertEquals(1, rows.length);
        assertEquals(5L, rows[0][0]);

        assertEquals("input line 00\n" +
                "input line 01\n" +
                "input line 02\n" +
                "input line 03\n" +
                "input line 04\n", TestingHelpers.readFile(uri));
    }

    @Test
    public void testToNestedStringObjectMap() throws Exception {

        Map<ColumnIdent, Object> columnIdentMap = new HashMap<>();
        columnIdentMap.put(new ColumnIdent("some", Arrays.asList("nested", "column")), "foo");
        Map<String, Object> convertedMap = WriterProjector.toNestedStringObjectMap(columnIdentMap);

        Map someMap = (Map) convertedMap.get("some");
        Map nestedMap = (Map) someMap.get("nested");
        assertThat((String)nestedMap.get("column"), is("foo"));
    }
}
