/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.engine.fetch;

import java.io.IOException;

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;

public abstract class FetchIdIterator {

    protected long idCount;

    public final void iterate(LongArrayList ids) throws IOException {

        this.idCount = ids.size();

        int currentShardId = -1;
        IndexReaderContext currentShardContext = null;
        int currentReaderCeiling = -1;
        int currentReaderDocBase = -1;

        for (LongCursor cursor : ids) {
            int shardId = FetchId.decodeReaderId(cursor.value);
            if (shardId != currentShardId) {
                currentShardId = shardId;
                currentShardContext = nextShard(shardId);
                currentReaderCeiling = -1;
            }
            int docId = FetchId.decodeDocId(cursor.value);
            if (docId >= currentReaderCeiling) {
                assert currentShardContext != null;
                int contextOrd = ReaderUtil.subIndex(docId, currentShardContext.leaves());
                var leafContext = nextReaderContext(contextOrd);
                currentReaderDocBase = leafContext.docBase;
                currentReaderCeiling = currentReaderDocBase + leafContext.reader().maxDoc();
            }

            if (collect(docId - currentReaderDocBase) == false) {
                break;
            }
        }
    }

    /**
     * Called when iterating to a new shard
     * @param shardId the id of the new shard
     * @return  a top-level IndexReaderContext for the new shard
     */
    protected abstract IndexReaderContext nextShard(int shardId);

    /**
     * Called when iterating to a new segment
     * @param contextOrd    the segment ord
     * @return the segment's LeafReaderContext
     */
    protected abstract LeafReaderContext nextReaderContext(int contextOrd) throws IOException;

    /**
     * Called when iterating to a new document
     * @param doc   the id of the document within the segment, relative to the segment's docbase
     * @return false if iteration should stop at this point
     */
    protected abstract boolean collect(int doc);

}
