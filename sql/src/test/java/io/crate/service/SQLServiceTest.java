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

import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.TransportSQLAction;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.junit.Test;

public class SQLServiceTest extends CrateUnitTest {

    @Test
    public void testDisableAndReEnable() throws Exception {
        InternalNode node = (InternalNode) NodeBuilder.nodeBuilder().local(true).data(true).settings(
                ImmutableSettings.builder()
                        .put(ClusterName.SETTING, getClass().getName())
                        .put("node.name", getClass().getName())
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0)
                        .put(EsExecutors.PROCESSORS, 1)
                        .put("http.enabled", false)
                        .put("index.store.type", "ram")
                        .put("config.ignore_system_properties", true)
                        .put("gateway.type", "none")).build();
        node.start();


        SQLService sqlService = node.injector().getInstance(SQLService.class);
        TransportSQLAction transportSQLAction = node.injector().getInstance(TransportSQLAction.class);
        transportSQLAction.execute(new SQLRequest("select name from sys.cluster")).actionGet();

        sqlService.disable();

        try {
            transportSQLAction.execute(new SQLRequest("select name from sys.cluster")).actionGet();
            fail("no exception thrown");
        } catch (NodeDisconnectedException e) {
            // success!
        }

        sqlService.start();
        transportSQLAction.execute(new SQLRequest("select name from sys.cluster")).actionGet();

        node.close();
    }
}