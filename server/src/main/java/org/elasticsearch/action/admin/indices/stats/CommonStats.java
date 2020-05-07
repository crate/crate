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

package org.elasticsearch.action.admin.indices.stats;

import org.apache.lucene.store.AlreadyClosedException;
import javax.annotation.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.StoreStats;

import java.io.IOException;

public class CommonStats implements Writeable {

    @Nullable
    public DocsStats docs;

    @Nullable
    public StoreStats store;

    public CommonStats() {
        this(CommonStatsFlags.NONE);
    }

    public CommonStats(CommonStatsFlags flags) {
        CommonStatsFlags.Flag[] setFlags = flags.getFlags();

        for (CommonStatsFlags.Flag flag : setFlags) {
            switch (flag) {
                case Docs:
                    docs = new DocsStats();
                    break;
                case Store:
                    store = new StoreStats();
                    break;
                default:
                    throw new IllegalStateException("Unknown Flag: " + flag);
            }
        }
    }

    public CommonStats(IndexShard indexShard, CommonStatsFlags flags) {
        CommonStatsFlags.Flag[] setFlags = flags.getFlags();
        for (CommonStatsFlags.Flag flag : setFlags) {
            try {
                switch (flag) {
                    case Docs:
                        docs = indexShard.docStats();
                        break;
                    case Store:
                        store = indexShard.storeStats();
                        break;
                    default:
                        throw new IllegalStateException("Unknown Flag: " + flag);
                }
            } catch (AlreadyClosedException e) {
                // shard is closed - no stats is fine
            }
        }
    }

    public CommonStats(StreamInput in) throws IOException {
        docs = in.readOptionalWriteable(DocsStats::new);
        store = in.readOptionalWriteable(StoreStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(docs);
        out.writeOptionalWriteable(store);
    }

    public void add(CommonStats stats) {
        if (docs == null) {
            if (stats.getDocs() != null) {
                docs = new DocsStats();
                docs.add(stats.getDocs());
            }
        } else {
            docs.add(stats.getDocs());
        }
        if (store == null) {
            if (stats.getStore() != null) {
                store = new StoreStats();
                store.add(stats.getStore());
            }
        } else {
            store.add(stats.getStore());
        }
    }

    @Nullable
    public DocsStats getDocs() {
        return this.docs;
    }

    @Nullable
    public StoreStats getStore() {
        return store;
    }
}
