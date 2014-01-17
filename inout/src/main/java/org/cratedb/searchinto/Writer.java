/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.searchinto;

import org.cratedb.action.searchinto.SearchIntoContext;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.Map;

public class Writer {

    private static final ESLogger logger = Loggers.getLogger(Writer.class);

    private final Map<String, WriterCollectorFactory> collectors;

    @Inject
    public Writer(Map<String, WriterCollectorFactory> collectors) {
        this.collectors = collectors;
    }


    public WriterResult execute(SearchIntoContext context) throws
            WriterException {
        logger.info("writing {}/{}", context.shardTarget().index(),
                context.shardTarget().getShardId());
        Query query = context.query();
        context.version(true);
        WriterCollector wc = collectors.get(context.targetType()).create(
                context);
        wc.open();
        try {
            context.searcher().search(query, wc);
        } catch (IOException e) {
            throw new WriterException(context, "Failed to write docs", e);
        }
        wc.close();
        WriterResult res = wc.getResult();
        logger.info("exported {} docs from {}/{}", res.getTotalWrites(),
                context.shardTarget().index(),
                context.shardTarget().getShardId());
        return res;


    }

}
