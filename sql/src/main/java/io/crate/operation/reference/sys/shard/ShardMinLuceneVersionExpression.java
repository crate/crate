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
package io.crate.operation.reference.sys.shard;

import io.crate.metadata.ReferenceImplementation;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.shard.IndexShard;

public class ShardMinLuceneVersionExpression implements ReferenceImplementation<BytesRef> {

    private IndexShard indexShard;

    public ShardMinLuceneVersionExpression(IndexShard indexShard) {
        this.indexShard = indexShard;
    }

    @Override
    public BytesRef value() {
        // This class is instantiated only once per shard so every query on sys.shards is calling value()
        // on the same object. We chose not to cache the value so in case you select min_lucene_version
        // multiple times in the same statement like:
        // `select min_lucene_version, min_lucene_version, ... from sys.shards`
        // you might get different results.
        try {
            return new BytesRef(indexShard.minimumCompatibleVersion().toString());
        } catch (AlreadyClosedException e) {
            return null;
        }
    }
}
