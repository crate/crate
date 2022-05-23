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

package io.crate.execution.engine.indexing;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper.SequenceIDFields;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
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
import org.openjdk.jmh.annotations.Warmup;

import io.crate.Streamer;
import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.data.Row;
import io.crate.execution.dml.upsert.InsertSourceGen;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Fork(value = 2)
@Measurement(iterations = 4)
public class IndexingBenchmark {

    private Node node;
    private SQLOperations sqlOperations;
    private ClusterService clusterService;
    private NodeContext nodeCtx;
    private Schemas schemas;
    private DocumentMapper documentMapper;
    private Document doc;
    private IndexWriter indexWriter;

    @Setup
    public void createEnv() throws Exception {
        Path tempDir = Files.createTempDirectory("");
        Settings settings = Settings.builder()
            .put("path.home", tempDir.toAbsolutePath().toString())
            .build();
        Environment environment = new Environment(settings, tempDir);
        List<Class<? extends Plugin>> plugins = List.of(Netty4Plugin.class);
        node = new Node(
            environment,
            plugins,
            true
        );
        node.start();
        Injector injector = node.injector();
        sqlOperations = injector.getInstance(SQLOperations.class);
        clusterService = injector.getInstance(ClusterService.class);
        nodeCtx = injector.getInstance(NodeContext.class);
        schemas = injector.getInstance(Schemas.class);
        IndicesService indices = injector.getInstance(IndicesService.class);

        String statement = "create table doc.users (id int primary key, name string)";
        var resultReceiver = new BaseResultReceiver();
        sqlOperations.newSystemSession()
            .quickExec(statement, resultReceiver, Row.EMPTY);
        resultReceiver.completionFuture().get(5, TimeUnit.SECONDS);

        IndexMetadata index = clusterService.state().getMetadata().index("users");
        IndexService indexService = indices.indexService(index.getIndex());
        documentMapper = indexService.mapperService().documentMapper();

        indexWriter = new IndexWriter(
            new ByteBuffersDirectory(),
            new IndexWriterConfig(new StandardAnalyzer())
        );
        doc = new Document();
        var intValueIndexer = new IntValueIndexer("id");
        intValueIndexer.indexValue(10, doc::add);
        var stringIndexer = new StringValueIndexer("name");
        stringIndexer.indexValue("Arthur", doc::add);
    }

    @Benchmark
    public long measure_index_writer_add_document() throws Exception {
        return indexWriter.addDocument(doc);
    }

    @Benchmark
    public long measure_index_writer_add_document_and_commit() throws Exception {
        indexWriter.addDocument(doc);
        return indexWriter.commit();
    }

    @Benchmark
    public ParsedDocument measure_indexing_using_source_generation_and_doc_mapper_parse() throws Exception {
        DocTableInfo table = schemas.getTableInfo(new RelationName("doc", "users"));
        InsertSourceGen sourceGen = InsertSourceGen.of(
            CoordinatorTxnCtx.systemTransactionContext(),
            nodeCtx,
            table,
            "users",
            false,
            List.copyOf(table.columns())
        );
        Object[] values = new Object[] { 10, "Arthur" };
        Map<String, Object> source = sourceGen.generateSourceAndCheckConstraints(values, List.of());
        BytesReference rawSource = BytesReference.bytes(XContentFactory.jsonBuilder().map(source));
        SourceToParse sourceToParse = new SourceToParse(
            "users",
            "10",
            rawSource,
            XContentType.JSON
        );
        return documentMapper.parse(sourceToParse);
    }

    @Benchmark
    public Document measure_indexing_using__indexers() throws Exception {
        DocTableInfo table = schemas.getTableInfo(new RelationName("doc", "users"));
        Object[] values = new Object[] { 10, "Arthur" };
        Indexer indexer = new Indexer(table.columns());
        return indexer.createDoc(values);
    }

    @Benchmark
    public ParsedDocument measure_indexing_using__indexers_creating_parsed_doc() throws Exception {
        DocTableInfo table = schemas.getTableInfo(new RelationName("doc", "users"));
        Object[] values = new Object[] { 10, "Arthur" };
        Indexer indexer = new Indexer(table.columns());
        Document doc = indexer.createDoc(values);
        InsertSourceGen sourceGen = InsertSourceGen.of(
            CoordinatorTxnCtx.systemTransactionContext(),
            nodeCtx,
            table,
            "users",
            false,
            List.copyOf(table.columns())
        );
        BytesReference source = sourceGen.generateSourceAndCheckConstraintsAsBytesReference(values);
        Field version = new NumericDocValuesField("_version", -1L);
        return new ParsedDocument(
            version,
            SequenceIDFields.emptySeqID(),
            "10",
            List.of(doc),
            source,
            (Mapping) null
        );
    }

    @Benchmark
    public ParsedDocument measure_indexing_using_indexers_creating_parsed_doc_with_binary_source() throws Exception {
        DocTableInfo table = schemas.getTableInfo(new RelationName("doc", "users"));
        Object[] values = new Object[] { 10, "Arthur" };
        Indexer indexer = new Indexer(table.columns());
        Document doc = indexer.createDoc(values);
        Iterator<Reference> it = table.columns().iterator();
        var out = new BytesStreamOutput();
        for (int i = 0; i < values.length; i++) {
            var value = values[i];
            Reference columnRef = it.next();
            Streamer<?> streamer = columnRef.valueType().streamer();
            ((Streamer) streamer).writeValueTo(out, value);
        }
        Field version = new NumericDocValuesField("_version", -1L);
        return new ParsedDocument(
            version,
            SequenceIDFields.emptySeqID(),
            "10",
            List.of(doc),
            out.bytes(),
            (Mapping) null
        );
    }
}
