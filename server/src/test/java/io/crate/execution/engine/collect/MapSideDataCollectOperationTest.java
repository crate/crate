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

package io.crate.execution.engine.collect;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static io.crate.testing.TestingHelpers.createReference;
import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.common.settings.Settings;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.analyze.CopyFromParserProperties;
import io.crate.data.BatchIterator;
import io.crate.data.CollectionBucket;
import io.crate.data.Row;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.execution.engine.collect.sources.FileCollectSource;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;
import io.crate.user.User;


public class MapSideDataCollectOperationTest extends CrateDummyClusterServiceUnitTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testFileUriCollect() throws Exception {
        FileCollectSource fileCollectSource = new FileCollectSource(
            createNodeContext(),
            clusterService,
            Collections.emptyMap(),
            THREAD_POOL,
            () -> List.of(User.CRATE_USER)
            );

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
            List.of("a", "b"),
            Arrays.asList(
                createReference("name", DataTypes.STRING),
                createReference(new ColumnIdent("details", "age"), DataTypes.INTEGER)
            ),
            Collections.emptyList(),
            null,
            false,
            CopyFromParserProperties.DEFAULT,
            FileUriCollectPhase.InputFormat.JSON,
            Settings.EMPTY
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        CollectTask collectTask = mock(CollectTask.class);
        BatchIterator<Row> iterator = fileCollectSource.getIterator(
            CoordinatorTxnCtx.systemTransactionContext(), collectNode, collectTask, false).get(5, TimeUnit.SECONDS);
        consumer.accept(iterator, null);
        assertThat(new CollectionBucket(consumer.getResult()), contains(
            isRow("Arthur", 38),
            isRow("Trillian", 33)
        ));
    }
}
