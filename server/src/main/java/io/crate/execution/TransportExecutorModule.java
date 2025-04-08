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

import org.elasticsearch.common.inject.AbstractModule;

import io.crate.execution.ddl.index.TransportSwapAndDropIndexNameAction;
import io.crate.execution.jobs.JobSetup;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.replication.logical.action.TransportAlterPublicationAction;
import io.crate.replication.logical.action.TransportCreatePublicationAction;
import io.crate.replication.logical.action.TransportCreateSubscriptionAction;
import io.crate.replication.logical.action.TransportDropPublicationAction;
import io.crate.statistics.TransportAnalyzeAction;

public class TransportExecutorModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(JobSetup.class).asEagerSingleton();
        bind(LuceneQueryBuilder.class).asEagerSingleton();

        bind(TransportSwapAndDropIndexNameAction.class).asEagerSingleton();
        bind(TransportAnalyzeAction.class).asEagerSingleton();

        bind(TransportCreatePublicationAction.class).asEagerSingleton();
        bind(TransportDropPublicationAction.class).asEagerSingleton();
        bind(TransportAlterPublicationAction.class).asEagerSingleton();
        bind(TransportCreateSubscriptionAction.class).asEagerSingleton();
    }
}
