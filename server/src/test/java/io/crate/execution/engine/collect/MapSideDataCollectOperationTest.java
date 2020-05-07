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

package io.crate.execution.engine.collect;

import io.crate.data.BatchIterator;
import io.crate.data.CollectionBucket;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.execution.engine.collect.sources.FileCollectSource;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.TestingRowConsumer;
import io.crate.types.DataTypes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static io.crate.testing.TestingHelpers.createReference;
import static io.crate.testing.TestingHelpers.getFunctions;
import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.mock;

public class MapSideDataCollectOperationTest extends CrateDummyClusterServiceUnitTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testFileUriCollect() throws Exception {
        Functions functions = getFunctions();
        FileCollectSource fileCollectSource = new FileCollectSource(functions, clusterService, Collections.emptyMap());

        File tmpFile = temporaryFolder.newFile("fileUriCollectOperation.json");
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tmpFile), StandardCharsets.UTF_8)) {
            writer.write("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}\n");
            writer.write("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}\n");
        }

        FileUriCollectPhase collectNode = new FileUriCollectPhase(
            UUID.randomUUID(),
            0,
            "test",
            Collections.singletonList("noop_id"),
            Literal.of(Paths.get(tmpFile.toURI()).toUri().toString()),
            Arrays.asList(
                createReference("name", DataTypes.STRING),
                createReference(new ColumnIdent("details", "age"), DataTypes.INTEGER)
            ),
            Collections.emptyList(),
            null,
            false,
            FileUriCollectPhase.InputFormat.JSON
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        CollectTask collectTask = mock(CollectTask.class);
        BatchIterator<Row> iterator = fileCollectSource.getIterator(
            CoordinatorTxnCtx.systemTransactionContext(), collectNode, collectTask, false);
        consumer.accept(iterator, null);
        assertThat(new CollectionBucket(consumer.getResult()), contains(
            isRow("Arthur", 38),
            isRow("Trillian", 33)
        ));
    }
}
