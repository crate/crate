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

import java.util.ArrayList;
import java.util.List;

public class FailedShardsException extends RuntimeException implements CrateException {

    public FailedShardsException(ShardOperationFailedException[] shardFailures) {
        super(genMessage(shardFailures));
    }

    private static String genMessage(ShardOperationFailedException[] shardFailures) {
        StringBuilder sb;

        if (shardFailures.length == 1) {
            sb = new StringBuilder("query failed on shard ");
        } else {
            sb = new StringBuilder("query failed on shards ");
        }

        List<String> errors = new ArrayList<>(shardFailures.length);
        String table = null;
        for (ShardOperationFailedException shardFailure : shardFailures) {
            if (shardFailure == null) {
                continue;
            }
            errors.add(shardFailure.shardId() + " ( " + shardFailure.reason() + " )");
            table = shardFailure.index();
        }

        if (errors.isEmpty() && table == null) {
            return "query failed on unknown shard / table";
        }
        sb.append(Joiner.on(", ").join(errors));
        if (table != null) {
            sb.append(" of table ").append(table);
        }
        return sb.toString();
    }

    @Override
    public int errorCode() {
        return 5002;
    }
}
