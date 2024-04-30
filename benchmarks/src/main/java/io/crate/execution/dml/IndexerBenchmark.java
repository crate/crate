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

package io.crate.execution.dml;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.transport.Netty4Plugin;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.Session;
import io.crate.action.sql.Sessions;
import io.crate.data.Row;
import io.crate.execution.dml.IndexItem.StaticItem;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2)
@Measurement(iterations = 5)
@Warmup(iterations = 3, time = 10000, timeUnit = TimeUnit.MILLISECONDS)
public class IndexerBenchmark {

    private Indexer indexer;
    private Node node;
    private List<StaticItem> items;

    @Setup
    public void setupIndexer() throws Exception {
        Path tempDir = Files.createTempDirectory("");
        Settings settings = Settings.builder()
            .put("path.home", tempDir.toAbsolutePath().toString())
            .build();
        Environment environment = new Environment(settings, tempDir);
        node = new Node(
            environment,
            List.of(Netty4Plugin.class),
            true
        );
        node.start();
        Injector injector = node.injector();

        Sessions sessions = injector.getInstance(Sessions.class);
        Session session = sessions.newSystemSession();
        String statement = """
            create table tbl (x int, y int)
            """;
        var resultReceiver = new BaseResultReceiver();
        session.quickExec(statement, resultReceiver, Row.EMPTY);
        resultReceiver.completionFuture().get(5, TimeUnit.SECONDS);

        Schemas schemas = injector.getInstance(Schemas.class);
        DocTableInfo table = schemas.getTableInfo(new RelationName("doc", "tbl"));

        indexer = new Indexer(
            table.concreteIndices(Metadata.EMPTY_METADATA)[0],
            table,
            new CoordinatorTxnCtx(session.sessionSettings()),
            injector.getInstance(NodeContext.class),
            List.of(
                table.getReference(new ColumnIdent("x")),
                table.getReference(new ColumnIdent("y"))
            ),
            null
        );
        items = IntStream.range(1, 2000).mapToObj(x -> new IndexItem.StaticItem(
            "dummy-" + x,
            List.of(),
            new Object[] { x, x * 2 },
            0,
            0
        )).toList();
    }

    @TearDown
    public void teardown() throws Exception {
        node.close();
    }

    @Benchmark
    public void measure_index(Blackhole blackhole) throws Exception {
        for (var item : items) {
            blackhole.consume(indexer.index(item));
        }
    }
}
