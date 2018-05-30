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

package io.crate.expression.reference.sys.shard;

import com.google.common.annotations.VisibleForTesting;
import io.crate.expression.NestableInput;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class ShardMinLuceneVersionExpression implements NestableInput<BytesRef> {


    private final LongSupplier docCount;
    private final Supplier<org.apache.lucene.util.Version> minimumCompatibleVersion;

    public ShardMinLuceneVersionExpression(IndexShard indexShard) {
        this(() -> indexShard.docStats().getCount(), indexShard::minimumCompatibleVersion);
    }

    @VisibleForTesting
    ShardMinLuceneVersionExpression(LongSupplier docCount,
                                    Supplier<org.apache.lucene.util.Version> minimumCompatibleVersion) {
        this.docCount = docCount;
        this.minimumCompatibleVersion = minimumCompatibleVersion;
    }

    @Override
    public BytesRef value() {
        long numDocs;
        try {
            numDocs = docCount.getAsLong();
        } catch (IllegalIndexShardStateException e) {
            return null;
        }
        if (numDocs == 0) {
            // If there are no documents we've no segments and `indexShard.minimumCompatibleVersion`
            // will return the version the index was created with.
            // That would cause `TableNeedsUpgradeSysCheck` to trigger a warning
            //
            // If a new segment is created in an empty shard it will use the newer lucene version
            return new BytesRef(Version.CURRENT.luceneVersion.toString());
        }

        // This class is instantiated only once per shard so every query on sys.shards is calling value()
        // on the same object. We chose not to cache the value so in case you select min_lucene_version
        // multiple times in the same statement like:
        // `select min_lucene_version, min_lucene_version, ... from sys.shards`
        // you might get different results.
        try {
            return new BytesRef(minimumCompatibleVersion.get().toString());
        } catch (AlreadyClosedException e) {
            return null;
        }
    }
}
