/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.exceptions;

import com.google.common.base.Joiner;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.List;

public class UnavailableShardsException extends RuntimeException implements CrateException {

    public UnavailableShardsException(ShardOperationFailedException[] shardFailures) {
        super(genMessage(shardFailures));
    }

    public UnavailableShardsException(ShardId shardId) {
        super(genMessage(shardId));
    }

    private static String genMessage(ShardId shardId) {
        return String.format("the shard %s of table %s is not available",
            shardId.getId(), shardId.getIndex());
    }

    private static String genMessage(ShardOperationFailedException[] shardFailures) {
        StringBuilder sb;

        if (shardFailures.length == 1) {
            sb = new StringBuilder("the shard ");
        } else {
            sb = new StringBuilder("the shards ");
        }

        List<Integer> shardIds = new ArrayList<>(shardFailures.length);
        for (ShardOperationFailedException shardFailure : shardFailures) {
            shardIds.add(shardFailure.shardId());
        }
        sb.append(Joiner.on(",").join(shardIds));
        if (shardFailures.length == 1) {
            sb.append(" is not available");
        } else {
            sb.append(" are not available");
        }
        return sb.toString();
    }

    @Override
    public int errorCode() {
        return 5002;
    }

    @Override
    public Object[] args() {
        return new Object[0];
    }
}
