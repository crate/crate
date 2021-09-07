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

package io.crate.execution;

import io.crate.cluster.decommission.TransportDecommissionNodeAction;
import io.crate.execution.ddl.TransportSchemaUpdateAction;
import io.crate.execution.ddl.TransportSwapRelationsAction;
import io.crate.execution.ddl.index.TransportSwapAndDropIndexNameAction;
import io.crate.execution.ddl.tables.TransportAlterTableAction;
import io.crate.execution.ddl.tables.TransportCloseTable;
import io.crate.execution.ddl.tables.TransportCreateTableAction;
import io.crate.execution.ddl.tables.TransportDropTableAction;
import io.crate.execution.ddl.tables.TransportOpenCloseTableOrPartitionAction;
import io.crate.execution.ddl.tables.TransportRenameTableAction;
import io.crate.execution.ddl.views.TransportCreateViewAction;
import io.crate.execution.ddl.views.TransportDropViewAction;
import io.crate.execution.dml.delete.TransportShardDeleteAction;
import io.crate.execution.dml.upsert.TransportShardUpsertAction;
import io.crate.execution.engine.collect.stats.TransportNodeStatsAction;
import io.crate.execution.engine.distribution.TransportDistributedResultAction;
import io.crate.execution.engine.fetch.TransportFetchNodeAction;
import io.crate.execution.engine.profile.TransportCollectProfileNodeAction;
import io.crate.execution.jobs.JobSetup;
import io.crate.execution.jobs.kill.TransportKillAllNodeAction;
import io.crate.execution.jobs.kill.TransportKillJobsNodeAction;
import io.crate.execution.jobs.transport.TransportJobAction;
import io.crate.expression.udf.TransportCreateUserDefinedFunctionAction;
import io.crate.expression.udf.TransportDropUserDefinedFunctionAction;
import io.crate.license.TransportSetLicenseAction;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.replication.logical.action.TransportCreatePublicationAction;
import io.crate.replication.logical.action.TransportCreateSubscriptionAction;
import io.crate.replication.logical.action.TransportDropPublicationAction;
import io.crate.statistics.TransportAnalyzeAction;
import org.elasticsearch.common.inject.AbstractModule;

public class TransportExecutorModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TransportActionProvider.class).asEagerSingleton();
        bind(JobSetup.class).asEagerSingleton();
        bind(LuceneQueryBuilder.class).asEagerSingleton();

        bind(TransportJobAction.class).asEagerSingleton();
        bind(TransportDistributedResultAction.class).asEagerSingleton();
        bind(TransportShardUpsertAction.class).asEagerSingleton();
        bind(TransportShardDeleteAction.class).asEagerSingleton();
        bind(TransportFetchNodeAction.class).asEagerSingleton();
        bind(TransportDecommissionNodeAction.class).asEagerSingleton();
        bind(TransportCollectProfileNodeAction.class).asEagerSingleton();
        bind(TransportKillAllNodeAction.class).asEagerSingleton();
        bind(TransportKillJobsNodeAction.class).asEagerSingleton();
        bind(TransportNodeStatsAction.class).asEagerSingleton();
        bind(TransportCreateTableAction.class).asEagerSingleton();
        bind(TransportRenameTableAction.class).asEagerSingleton();
        bind(TransportSwapAndDropIndexNameAction.class).asEagerSingleton();
        bind(TransportOpenCloseTableOrPartitionAction.class).asEagerSingleton();
        bind(TransportCloseTable.class).asEagerSingleton();
        bind(TransportDropTableAction.class).asEagerSingleton();
        bind(TransportCreateUserDefinedFunctionAction.class).asEagerSingleton();
        bind(TransportDropUserDefinedFunctionAction.class).asEagerSingleton();
        bind(TransportSchemaUpdateAction.class).asEagerSingleton();
        bind(TransportCreateViewAction.class).asEagerSingleton();
        bind(TransportDropViewAction.class).asEagerSingleton();
        bind(TransportSwapRelationsAction.class).asEagerSingleton();
        bind(TransportAlterTableAction.class).asEagerSingleton();
        bind(TransportAnalyzeAction.class).asEagerSingleton();
        bind(TransportSetLicenseAction.class).asEagerSingleton();

        bind(TransportCreatePublicationAction.class).asEagerSingleton();
        bind(TransportDropPublicationAction.class).asEagerSingleton();
        bind(TransportCreateSubscriptionAction.class).asEagerSingleton();
    }
}
