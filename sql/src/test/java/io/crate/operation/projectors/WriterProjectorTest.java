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
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.Row1;
import io.crate.exceptions.UnhandledServerException;
import io.crate.jobs.ExecutionState;
import io.crate.metadata.ColumnIdent;
import io.crate.operation.collect.CollectExpression;
import io.crate.planner.projection.WriterProjection;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.TestingHelpers;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class WriterProjectorTest extends CrateUnitTest {

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testWriteRawToFile() throws Exception {

        String fileAbsolutePath = folder.newFile("out.json").getAbsolutePath();
        String uri = Paths.get(fileAbsolutePath).toUri().toString();
        WriterProjector projector = new WriterProjector(
                executorService,
                uri,
                null,
                null,
                ImmutableSet.<CollectExpression<Row, ?>>of(),
                new HashMap<ColumnIdent, Object>(),
                null,
                WriterProjection.OutputFormat.JSON_OBJECT
        );
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        projector.downstream(rowReceiver);

        projector.prepare(mock(ExecutionState.class));
        for (int i = 0; i < 5; i++) {
            projector.setNextRow(new Row1(new BytesRef(String.format(Locale.ENGLISH, "input line %02d", i))));
        }
        projector.finish();

        Bucket rows = rowReceiver.result();
        assertThat(rows, contains(isRow(5L)));

        assertEquals("input line 00\n" +
                "input line 01\n" +
                "input line 02\n" +
                "input line 03\n" +
                "input line 04\n", TestingHelpers.readFile(fileAbsolutePath));
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

    @Test
    public void testDirectoryAsFile() throws Exception {
        expectedException.expect(UnhandledServerException.class);
        expectedException.expectMessage("Failed to open output: 'Output path is a directory: ");

        String uri = Paths.get(folder.newFolder().toURI()).toUri().toString();
        WriterProjector projector = new WriterProjector(
                executorService,
                uri,
                null,
                null,
                ImmutableSet.<CollectExpression<Row, ?>>of(),
                new HashMap<ColumnIdent, Object>(),
                null,
                WriterProjection.OutputFormat.JSON_OBJECT
        );
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        projector.downstream(rowReceiver);
        projector.prepare(mock(ExecutionState.class));
        projector.finish();
        rowReceiver.result();
    }

    @Test
    public void testFileAsDirectory() throws Exception {
        expectedException.expect(UnhandledServerException.class);
        expectedException.expectMessage("Failed to open output");

        String uri = Paths.get(folder.newFile().toURI()).resolve("out.json").toUri().toString();
        WriterProjector projector = new WriterProjector(
                executorService,
                uri,
                null,
                null,
                ImmutableSet.<CollectExpression<Row, ?>>of(),
                new HashMap<ColumnIdent, Object>(),
                null,
                WriterProjection.OutputFormat.JSON_OBJECT
        );
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        projector.downstream(rowReceiver);
        projector.prepare(mock(ExecutionState.class));
        projector.finish();
        rowReceiver.result();
    }
}
