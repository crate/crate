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

package io.crate.execution;

import io.crate.execution.ddl.TransportDropTableAction;
import io.crate.execution.ddl.TransportOpenCloseTableOrPartitionAction;
import io.crate.execution.ddl.TransportRenameTableAction;
import io.crate.execution.ddl.views.TransportCreateViewAction;
import io.crate.execution.dml.delete.TransportShardDeleteAction;
import io.crate.execution.dml.upsert.TransportShardUpsertAction;
import io.crate.execution.engine.collect.stats.TransportNodeStatsAction;
import io.crate.execution.engine.distribution.TransportDistributedResultAction;
import io.crate.execution.engine.fetch.TransportFetchNodeAction;
import io.crate.execution.jobs.ContextPreparer;
import io.crate.execution.jobs.kill.TransportKillAllNodeAction;
import io.crate.execution.jobs.kill.TransportKillJobsNodeAction;
import io.crate.execution.jobs.transport.TransportJobAction;
import io.crate.lucene.LuceneQueryBuilder;
import org.elasticsearch.common.inject.AbstractModule;

public class TransportExecutorModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TransportActionProvider.class).asEagerSingleton();
        bind(ContextPreparer.class).asEagerSingleton();
        bind(LuceneQueryBuilder.class).asEagerSingleton();

        bind(TransportJobAction.class).asEagerSingleton();
        bind(TransportDistributedResultAction.class).asEagerSingleton();
        bind(TransportShardUpsertAction.class).asEagerSingleton();
        bind(TransportShardDeleteAction.class).asEagerSingleton();
        bind(TransportFetchNodeAction.class).asEagerSingleton();
        bind(TransportKillAllNodeAction.class).asEagerSingleton();
        bind(TransportKillJobsNodeAction.class).asEagerSingleton();
        bind(TransportNodeStatsAction.class).asEagerSingleton();
        bind(TransportRenameTableAction.class).asEagerSingleton();
        bind(TransportOpenCloseTableOrPartitionAction.class).asEagerSingleton();
        bind(TransportDropTableAction.class).asEagerSingleton();
        bind(TransportCreateViewAction.class).asEagerSingleton();
    }
}
