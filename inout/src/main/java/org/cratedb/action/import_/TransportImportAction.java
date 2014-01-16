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
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.cratedb.action.import_;

import org.cratedb.action.import_.parser.ImportParser;
import org.cratedb.import_.Importer;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 *
 */
public class TransportImportAction extends AbstractTransportImportAction {

    @Inject
    public TransportImportAction(Settings settings, ClusterName clusterName,
                                         ThreadPool threadPool, ClusterService clusterService,
                                         TransportService transportService, ImportParser importParser, Importer importer, NodeEnvironment nodeEnv) {
        super(settings, clusterName, threadPool, clusterService, transportService, importParser, importer, nodeEnv);
    }

    @Override
    protected String transportAction() {
        return ImportAction.NAME;
    }
}
