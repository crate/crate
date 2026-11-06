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

package io.crate.statistics;


import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.common.collections.Lists;
import io.crate.common.io.IOUtils;
import io.crate.execution.engine.fetch.FetchId;

public class ReservoirTest extends ESTestCase {

    @Test
    public void test_sampling() {
        Random random = new Random(42);
        Reservoir samples = new Reservoir(5, random);
        for (int i = 0; i < 100; i++) {
            samples.update(i);
        }
        assertThat(samples.samples().buffer).containsExactly(13, 18, 38, 50, 83, 0L, 0L, 0L, 0L, 0L);
    }

    @Test
    public void test_reservoir_is_protected_against_integer_overflow() throws Exception {
        Random random = new Random(42);

        Reservoir samples = new Reservoir(5, random);
        Field f1 = samples.getClass().getDeclaredField("itemsSeen");
        f1.setAccessible(true);
        int itemsSeen = Integer.MAX_VALUE;
        f1.set(samples, itemsSeen);
        samples.update(6L);

        assertThat(samples.samples()).isEmpty();
    }

    @Test
    public void test_sample_ids_are_reported_in_order() throws Exception {
        int numSearches = random().nextInt(3, 10);
        List<Directory> dirs = new ArrayList<>();
        Document doc = new Document();
        for (int i = 0; i < numSearches; i++) {
            Directory d = newDirectory();
            RandomIndexWriter iw = new RandomIndexWriter(random(), d);
            for (int j = 0; j < 200; j++) {
                doc.clear();
                doc.add(new LongField("long", 1, Store.NO));
                iw.addDocument(doc);
            }
            iw.close();
            dirs.add(d);
        }

        Reservoir r = new Reservoir(30, random());

        int readerIdx = 0;
        for (Directory d : dirs) {
            try (IndexReader ir = DirectoryReader.open(d)) {
                IndexSearcher searcher = new IndexSearcher(ir);
                ReservoirSampler.sampleDocIds(r, readerIdx, searcher);
            }
            readerIdx++;
        }

        var sortedSamples = r.samples();
        long lastId = -1;
        for (var v : sortedSamples) {
            assertThat(v.value).isGreaterThan(lastId);
            lastId = v.value;
        }

        IOUtils.close(dirs);
    }

    @Test
    public void test_sample_live_doc_ids_returns_sorted_distinct_live_docs() throws Exception {
        int docsPerSegment = 100;
        List<Directory> dirs = List.of(
            newIndex(3, docsPerSegment),
            newIndex(0, docsPerSegment),
            newIndex(2, docsPerSegment),
            newIndex(4, docsPerSegment),
            newIndex(0, docsPerSegment)
        );
        List<IndexReader> readers = new ArrayList<>();
        try {
            for (Directory dir : dirs) {
                readers.add(DirectoryReader.open(dir));
            }
            // 9 segments with 50 live docs each; 300 of 450 forces duplicate draws
            int numSamples = 300;
            long[] fetchIds = ReservoirSampler.sampleLiveDocIds(
                Lists.map(readers, IndexSearcher::new),
                numSamples,
                random()
            );

            assertThat(fetchIds).hasSize(numSamples);
            long lastId = -1;
            for (long fetchId : fetchIds) {
                assertThat(fetchId).isGreaterThan(lastId);
                lastId = fetchId;
                IndexReader reader = readers.get(FetchId.decodeReaderId(fetchId));
                Document doc = reader.storedFields().document(FetchId.decodeDocId(fetchId));
                assertThat(doc.getField("value").numericValue().longValue())
                    .as("deleted docs have a value below %d", docsPerSegment / 2)
                    .isGreaterThanOrEqualTo(docsPerSegment / 2);
            }
        } finally {
            IOUtils.close(readers);
            IOUtils.close(dirs);
        }
    }

    /// Creates an index with `numSegments` segments, in which the first half of the docs of each segment is deleted.
    private Directory newIndex(int numSegments, int docsPerSegment) throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            for (int segment = 0; segment < numSegments; segment++) {
                for (int i = 0; i < docsPerSegment; i++) {
                    Document doc = new Document();
                    doc.add(new LongField("value", i, Store.YES));
                    writer.addDocument(doc);
                }
                writer.commit();
            }
            writer.deleteDocuments(LongPoint.newRangeQuery("value", 0, docsPerSegment / 2 - 1));
        }
        return dir;
    }
}
