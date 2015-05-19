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

package io.crate.service;

import io.crate.action.sql.TransportSQLAction;
import io.crate.action.sql.TransportSQLBulkAction;
import io.crate.operation.collect.StatsTables;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

public class SQLService extends AbstractLifecycleComponent<SQLService> {

    private final StatsTables statsTables;
    private final TransportSQLAction transportSQLAction;
    private final TransportSQLBulkAction transportSQLBulkAction;

    @Inject
    public SQLService(Settings settings,
                      StatsTables statsTables,
                      TransportSQLAction transportSQLAction,
                      TransportSQLBulkAction transportSQLBulkAction) {
        super(settings);
        this.statsTables = statsTables;
        this.transportSQLAction = transportSQLAction;
        this.transportSQLBulkAction = transportSQLBulkAction;
    }

    @Override
    protected void doStart() throws ElasticsearchException {

    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {

    }

    @Override
    protected void doEnable() throws ElasticsearchException {
        transportSQLAction.enable();
        transportSQLBulkAction.enable();
    }

    @Override
    protected void doDisable() throws ElasticsearchException {
        transportSQLAction.disable();
        transportSQLBulkAction.disable();
        while (statsTables.activeRequests() > 0) {
            logger.debug("still processing {} SQL requests...", statsTables.activeRequests());
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
        }
    }
}
