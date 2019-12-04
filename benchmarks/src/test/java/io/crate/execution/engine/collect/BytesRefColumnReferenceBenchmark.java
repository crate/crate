/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.collect;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.plain.SortedSetDVOrdinalsIndexFieldData;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class BytesRefColumnReferenceBenchmark {

    private Iterator<LeafReaderContext> leavesIt;
    private Weight weight;
    private SortedSetDVOrdinalsIndexFieldData indexFieldData;

    @Setup
    public void prepare() throws Exception {
        IndexWriter iw = new IndexWriter(new ByteBuffersDirectory(), new IndexWriterConfig(new StandardAnalyzer()));
        String columnName = "x";
        for (long i = 0; i < 100_000; i++) {
            Document doc = new Document();
            String val = "val_" + i;
            doc.add(new SortedSetDocValuesField(columnName, new BytesRef(val)));
            iw.addDocument(doc);
        }
        iw.commit();
        IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(iw));
        List<LeafReaderContext> leaves = indexSearcher.getTopReaderContext().leaves();
        leavesIt = leaves.iterator();
        weight = indexSearcher.createWeight(indexSearcher.rewrite(new MatchAllDocsQuery()), ScoreMode.COMPLETE_NO_SCORES, 1f);

        IndexMetaData indexMetaData = IndexMetaData.builder("t1")
            .settings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetaData, Settings.EMPTY);
        indexFieldData = new SortedSetDVOrdinalsIndexFieldData(indexSettings, null, columnName, new NoneCircuitBreakerService());
    }

    @Benchmark
    public void loadBytesValuesAndOrdinalsValues() throws IOException {
        while (leavesIt.hasNext()) {
            LeafReaderContext leaf = leavesIt.next();
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            SortedSetDocValues sortedSetDocValues = indexFieldData.load(leaf).getOrdinalsValues();
            SortedBinaryDocValues sortedBinaryDocValues = indexFieldData.load(leaf).getBytesValues();
        }

    }

    @Benchmark
    public void loadBytesValuesUsingOrdinalsValues() throws IOException {
        while (leavesIt.hasNext()) {
            LeafReaderContext leaf = leavesIt.next();
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            SortedSetDocValues sortedSetDocValues = indexFieldData.load(leaf).getOrdinalsValues();
            SortedBinaryDocValues sortedBinaryDocValues = FieldData.toString(sortedSetDocValues);
        }
    }
}
