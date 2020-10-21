/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ShuffleForcedMergePolicyTests extends BaseMergePolicyTestCase {
    public void testDiagnostics() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            MergePolicy mp = new ShuffleForcedMergePolicy(newLogMergePolicy());
            iwc.setMergePolicy(mp);
            boolean sorted = random().nextBoolean();
            if (sorted) {
                iwc.setIndexSort(new Sort(new SortField("sort", SortField.Type.INT)));
            }
            int numDocs = atLeast(100);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < numDocs; i++) {
                    if (i % 10 == 0) {
                        writer.flush();
                    }
                    Document doc = new Document();
                    doc.add(new StringField("id", "" + i, Field.Store.NO));
                    doc.add(new NumericDocValuesField("sort", random().nextInt()));
                    writer.addDocument(doc);
                }
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertThat(reader.leaves().size(), greaterThan(2));
                    assertSegmentReaders(reader, leaf -> {
                        assertFalse(ShuffleForcedMergePolicy.isInterleavedSegment(leaf));
                    });
                }
                writer.forceMerge(1);
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertThat(reader.leaves().size(), equalTo(1));
                    assertSegmentReaders(reader, leaf -> {
                        assertTrue(ShuffleForcedMergePolicy.isInterleavedSegment(leaf));
                    });
                }
            }
        }
    }

    private void assertSegmentReaders(DirectoryReader reader, Consumer<LeafReader> checkLeaf) {
        for (LeafReaderContext leaf : reader.leaves()) {
            checkLeaf.accept(leaf.reader());
        }
    }

    @Override
    protected MergePolicy mergePolicy() {
        return new ShuffleForcedMergePolicy(newLogMergePolicy());
    }

    @Override
    protected void assertSegmentInfos(MergePolicy policy, SegmentInfos infos) throws IOException {}

    @Override
    protected void assertMerge(MergePolicy policy, MergePolicy.MergeSpecification merge) throws IOException {}
}
