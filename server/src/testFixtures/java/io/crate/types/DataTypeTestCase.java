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

package io.crate.types;

import static io.crate.testing.Asserts.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.junit.Test;

import io.crate.Streamer;
import io.crate.execution.dml.ValueIndexer;
import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.DataTypeTesting;
import io.crate.testing.IndexEnv;
import io.crate.testing.SQLExecutor;


public abstract class DataTypeTestCase<T> extends CrateDummyClusterServiceUnitTest {

    public abstract DataType<T> getType();

    @Test
    public void test_doc_values_write_and_read_roundtrip_inclusive_doc_mapper_parse() throws Exception {
        DataType<T> type = getType();
        StorageSupport<? super T> storageSupport = type.storageSupport();
        if (storageSupport == null) {
            return;
        }
        var sqlExecutor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (id int, x " + type.getTypeSignature().toString() + ")")
            .build();

        Supplier<T> dataGenerator = DataTypeTesting.getDataGenerator(type);
        DocTableInfo table = sqlExecutor.resolveTableInfo("tbl");
        Reference reference = table.getReference(new ColumnIdent("x"));
        assertThat(reference).isNotNull();

        try (var indexEnv = new IndexEnv(
                THREAD_POOL,
                table,
                clusterService.state(),
                Version.CURRENT,
                createTempDir())) {
            T value = dataGenerator.get();

            MapperService mapperService = indexEnv.mapperService();
            ValueIndexer<? super T> valueIndexer = storageSupport.valueIndexer(
                table.ident(),
                reference,
                column -> mapperService.getLuceneFieldType(column.fqn()),
                table::getReference
            );

            IndexWriter writer = indexEnv.writer();
            try (XContentBuilder xContentBuilder = XContentFactory.json(new BytesStreamOutput())) {
                List<IndexableField> fields = new ArrayList<>();
                xContentBuilder.startObject()
                    .field(reference.column().fqn());
                valueIndexer.indexValue(
                    value,
                    xContentBuilder,
                    fields::add,
                    Map.of(),
                    Map.of()
                );
                xContentBuilder.endObject();
                writer.addDocument(fields);
                writer.commit();

                // going through the document mapper must create the same fields
                String id = "1";
                ParsedDocument parsedDocument = mapperService.documentMapper().parse(new SourceToParse(
                    table.ident().indexNameOrAlias(),
                    id,
                    BytesReference.bytes(xContentBuilder),
                    XContentType.JSON
                ));
                IndexableField[] fieldsFromMapper = parsedDocument.doc().getFields("x");
                assertThat(fieldsFromMapper).hasSize(fields.size());
                for (int i = 0; i < fields.size(); i++) {
                    var field1 = fields.get(i);
                    var field2 = fieldsFromMapper[i];
                    assertThat(field1.binaryValue()).isEqualTo(field2.binaryValue());
                    assertThat(field1.stringValue()).isEqualTo(field2.stringValue());
                    assertThat(field1.numericValue()).isEqualTo(field2.numericValue());
                }
            }

            LuceneReferenceResolver luceneReferenceResolver = indexEnv.luceneReferenceResolver();
            LuceneCollectorExpression<?> docValueImpl = luceneReferenceResolver.getImplementation(reference);
            LuceneCollectorExpression<?> sourceLookup = luceneReferenceResolver.getImplementation(DocReferences.toSourceLookup(reference));
            assertThat(docValueImpl).isNotNull();
            assertThat(sourceLookup).isNotNull();

            IndexSearcher indexSearcher;
            try {
                indexSearcher = new IndexSearcher(DirectoryReader.open(writer));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            List<LeafReaderContext> leaves = indexSearcher.getTopReaderContext().leaves();
            assertThat(leaves).hasSize(1);
            LeafReaderContext leafReader = leaves.get(0);

            Weight weight = indexSearcher.createWeight(
                new MatchAllDocsQuery(),
                ScoreMode.COMPLETE_NO_SCORES,
                1.0f
            );

            Scorer scorer = weight.scorer(leafReader);
            CollectorContext collectorContext = new CollectorContext(1, Set.of(), table.lookupNameBySourceKey());
            ReaderContext readerContext = new ReaderContext(leafReader);
            DocIdSetIterator iterator = scorer.iterator();
            int nextDoc = iterator.nextDoc();

            docValueImpl.startCollect(collectorContext);
            docValueImpl.setNextReader(readerContext);
            docValueImpl.setNextDocId(nextDoc);
            assertThat(docValueImpl.value()).isEqualTo(value);
        }
    }

    @Test
    public void test_type_streaming_roundtrip() throws Exception {
        DataType<T> type = getType();
        BytesStreamOutput out = new BytesStreamOutput();
        DataTypes.toStream(type, out);

        StreamInput in = out.bytes().streamInput();
        DataType<?> fromStream = DataTypes.fromStream(in);
        assertThat(fromStream.id()).isEqualTo(type.id());
        assertThat(fromStream.characterMaximumLength()).isEqualTo(type.characterMaximumLength());
    }

    @Test
    public void test_value_streaming_roundtrip() throws Exception {
        DataType<T> type = getType();
        Supplier<T> dataGenerator = DataTypeTesting.getDataGenerator(type);
        T value = dataGenerator.get();

        Streamer<T> streamer = type.streamer();
        BytesStreamOutput out = new BytesStreamOutput();
        streamer.writeValueTo(out, value);
        streamer.writeValueTo(out, null);

        StreamInput in = out.bytes().streamInput();
        assertThat(type.compare(streamer.readValueFrom(in), value)).isEqualTo(0);
        assertThat(streamer.readValueFrom(in)).isNull();
    }
}
