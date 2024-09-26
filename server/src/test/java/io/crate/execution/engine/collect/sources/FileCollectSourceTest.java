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

package io.crate.execution.engine.collect.sources;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.analyze.CopyFromParserProperties;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.execution.dsl.phases.FileUriCollectPhase.InputFormat;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.engine.collect.CollectTask;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import io.crate.role.Role;

public class FileCollectSourceTest extends CrateDummyClusterServiceUnitTest {


    @Test
    public void test_file_collect_source_returns_iterator_that_can_skip_lines() throws Exception {
        List<String> targetColumns = List.of();
        List<Projection> projections = List.of();
        List<Symbol> toCollect = List.of(
            TestingHelpers.createReference("_raw", DataTypes.STRING)
        );
        Path tmpFile = createTempFile("tempfile1", ".csv");
        Files.write(tmpFile, List.of(
            "garbage1",
            "garbage2",
            "x,y",
            "1,2",
            "10,20"
        ));
        FileUriCollectPhase fileUriCollectPhase = new FileUriCollectPhase(
            UUID.randomUUID(),
            1,
            "copy from",
            List.of(),
            Literal.of(tmpFile.toUri().toString()),
            targetColumns,
            toCollect,
            projections,
            null,
            false,
            new CopyFromParserProperties(true, true, ',', 2),
            InputFormat.CSV,
            Settings.EMPTY
        );

        FileCollectSource fileCollectSource = new FileCollectSource(
            createNodeContext(),
            clusterService,
            Map.of(),
            Map.of(),
            THREAD_POOL,
            () -> List.of(Role.CRATE_USER)
        );

        CompletableFuture<BatchIterator<Row>> iterator = fileCollectSource.getIterator(
            CoordinatorTxnCtx.systemTransactionContext(),
            fileUriCollectPhase,
            mock(CollectTask.class),
            false
        );
        assertThat(iterator).succeedsWithin(5, TimeUnit.SECONDS);
        CompletableFuture<List<Object>> resultFuture = iterator.join()
            .map(row -> row.get(0))
            .toList();

        assertThat(resultFuture).succeedsWithin(5, TimeUnit.SECONDS);
        assertThat(resultFuture.join()).containsExactly(
            "{\"x\":\"1\",\"y\":\"2\"}",
            "{\"x\":\"10\",\"y\":\"20\"}"
        );
    }
}

