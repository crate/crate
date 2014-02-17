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

package org.cratedb.service;

import org.cratedb.action.TransportDistributedSQLAction;
import org.cratedb.action.TransportSQLReduceHandler;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

public class SQLService extends AbstractLifecycleComponent<SQLService> {

    private final TransportSQLReduceHandler transportSQLReduceHandler;
    private final TransportDistributedSQLAction transportDistributedSQLAction;

    public static final String CUSTOM_ANALYSIS_SETTINGS_PREFIX = "crate.analysis.custom";

    @Inject
    public SQLService(Settings settings,
                      TransportSQLReduceHandler transportSQLReduceHandler,
                      TransportDistributedSQLAction transportDistributedSQLAction) {
        super(settings);
        this.transportSQLReduceHandler = transportSQLReduceHandler;
        this.transportDistributedSQLAction = transportDistributedSQLAction;
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        transportSQLReduceHandler.registerHandler();
        transportDistributedSQLAction.registerHandler();
    }

    @Override
    protected void doStop() throws ElasticSearchException {

    }

    @Override
    protected void doClose() throws ElasticSearchException {

    }
}
