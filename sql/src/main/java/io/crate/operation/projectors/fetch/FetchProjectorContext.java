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

package io.crate.operation.projectors.fetch;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import io.crate.Streamer;
import io.crate.analyze.symbol.Symbols;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.TableIdent;
import io.crate.operation.reference.doc.lucene.FetchIds;
import io.crate.planner.node.fetch.FetchSource;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FetchProjectorContext {

    final Map<String, IntSet> nodeToReaderIds;
    final Map<TableIdent, FetchSource> tableToFetchSource;

    private final TreeMap<Integer, String> readerIdToIndex;
    private final Map<String, TableIdent> indexToTable;
    private final IntObjectHashMap<ReaderBucket> readerBuckets = new IntObjectHashMap<>();
    private Map<String, IntObjectHashMap<Streamer[]>> nodeIdToReaderIdToStreamers;

    public FetchProjectorContext(Map<TableIdent, FetchSource> tableToFetchSource,
                                 Map<String, IntSet> nodeToReaderIds,
                                 TreeMap<Integer, String> readerIdToIndex,
                                 Map<String, TableIdent> indexToTable) {
        this.tableToFetchSource = tableToFetchSource;
        this.nodeToReaderIds = nodeToReaderIds;
        this.readerIdToIndex = readerIdToIndex;
        this.indexToTable = indexToTable;
    }


    ReaderBucket readerBucket(int readerId) {
        return readerBuckets.get(readerId);
    }

    ReaderBucket require(long fetchId) {
        int readerId = FetchIds.extractReaderId(fetchId);
        int docId = FetchIds.extractDocId(fetchId);
        ReaderBucket readerBucket = readerBuckets.get(readerId);
        if (readerBucket == null) {
            readerBucket = createReaderBucket(readerId);
            readerBuckets.put(readerId, readerBucket);
        }
        readerBucket.require(docId);
        return readerBucket;
    }

    private ReaderBucket createReaderBucket(int readerId) {
        String index = readerIdToIndex.floorEntry(readerId).getValue();
        TableIdent tableIdent = indexToTable.get(index);
        FetchSource fetchSource = tableToFetchSource.get(tableIdent);
        assert fetchSource != null : "fetchSource must be available";
        return new ReaderBucket(!fetchSource.references().isEmpty(), partitionValues(index, fetchSource.partitionedByColumns()));

    }

    private Object[] partitionValues(String index, List<Reference> partitionByColumns) {
        if (partitionByColumns.isEmpty()) {
            return null;
        }
        PartitionName pn = PartitionName.fromIndexOrTemplate(index);
        List<BytesRef> partitionRowValues = pn.values();
        Object[] partitionValues = new Object[partitionRowValues.size()];
        for (int i = 0; i < partitionRowValues.size(); i++) {
            partitionValues[i] = partitionByColumns.get(i).valueType().value(partitionRowValues.get(i));
        }
        return partitionValues;
    }

    ReaderBucket getReaderBucket(int readerId) {
        return readerBuckets.get(readerId);
    }

    @Nullable
    private FetchSource getFetchSource(int readerId) {
        String index = readerIdToIndex.floorEntry(readerId).getValue();
        TableIdent tableIdent = indexToTable.get(index);
        return tableToFetchSource.get(tableIdent);
    }

    public Map<String, ? extends IntObjectMap<Streamer[]>> nodeIdsToStreamers() {
        if (nodeIdToReaderIdToStreamers == null) {
            nodeIdToReaderIdToStreamers = new HashMap<>(nodeToReaderIds.size(), 1.0f);
            for (Map.Entry<String, IntSet> entry : nodeToReaderIds.entrySet()) {
                IntObjectHashMap<Streamer[]> readerIdsToStreamers = new IntObjectHashMap<>();
                nodeIdToReaderIdToStreamers.put(entry.getKey(), readerIdsToStreamers);

                for (IntCursor readerIdCursor : entry.getValue()) {
                    FetchSource fetchSource = getFetchSource(readerIdCursor.value);
                    if (fetchSource == null) {
                        continue;
                    }
                    readerIdsToStreamers.put(readerIdCursor.value, Symbols.streamerArray(fetchSource.references()));
                }
            }
        }
        return nodeIdToReaderIdToStreamers;
    }

    void clearBuckets() {
        for (ObjectCursor<ReaderBucket> bucketCursor : readerBuckets.values()) {
            bucketCursor.value.docs.clear();
        }
    }
}
