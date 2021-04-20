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

package org.elasticsearch.index.store;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;

import java.nio.file.Path;

public final class StoreUtils {

    /**
     * Returns {@code true} iff the given location contains an index an the index
     * can be successfully opened. This includes reading the segment infos and possible
     * corruption markers.
     */
    public static boolean canOpenIndex(Logger logger, Path indexLocation,
                                            ShardId shardId, NodeEnvironment.ShardLocker shardLocker) {
        try {
            Store.tryOpenIndex(indexLocation, shardId, shardLocker, logger);
        } catch (Exception ex) {
            logger.trace(() -> new ParameterizedMessage("Can't open index for path [{}]", indexLocation), ex);
            return false;
        }
        return true;
    }
}
