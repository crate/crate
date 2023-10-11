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

package io.crate.exceptions;

import io.crate.metadata.RelationName;
import org.elasticsearch.index.shard.ShardId;

import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

public class UnavailableShardsException extends RuntimeException implements TableScopeException {

    private final RelationName relationName;

    public UnavailableShardsException(ShardId shardId) {
        super(genMessage(shardId));
        this.relationName = RelationName.fromIndexName(shardId.getIndexName());
    }

    private static String genMessage(ShardId shardId) {
        return String.format(Locale.ENGLISH, "the shard %s of table %s is not available",
            shardId.id(), shardId.getIndex());
    }

    @Override
    public Collection<RelationName> getTableIdents() {
        return Collections.singletonList(relationName);
    }
}
