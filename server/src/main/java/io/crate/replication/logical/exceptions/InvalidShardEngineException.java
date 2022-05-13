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

package io.crate.replication.logical.exceptions;

import java.util.Locale;

import org.elasticsearch.index.shard.ShardId;

/**
 * Marker exception raised if the engine of a target shard is not a
 * {@link io.crate.replication.logical.engine.SubscriberEngine}.
 * This is expected to happen if a subscription is dropped and an index is turned into a normal read-write
 * engine while {@link io.crate.replication.logical.action.ReplayChangesAction} responses are still arriving.
 */
public class InvalidShardEngineException extends RuntimeException {

    private static final String MESSAGE = "Shard '%s' is not subscribed anymore (wrong engine)";

    public InvalidShardEngineException(ShardId shardId) {
        super(String.format(Locale.ENGLISH, MESSAGE, shardId));
    }
}
