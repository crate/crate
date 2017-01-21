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

package io.crate.monitor;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;

public class MonitorModule extends AbstractModule {

    public final static String NODE_INFO_EXTENDED_TYPE = "node.info.extended.type";
    private final static String NODE_INFO_EXTENDED_DEFAULT_TYPE = "none";
    private final static Logger LOGGER = Loggers.getLogger(MonitorModule.class);

    private final Settings settings;
    private final Map<String, Class<? extends ExtendedNodeInfo>> extendedNodeInfoTypes = new HashMap<>();

    public MonitorModule(Settings settings) {
        this.settings = settings;
        addExtendedNodeInfoType("none", ZeroExtendedNodeInfo.class);
    }

    public void addExtendedNodeInfoType(String type, Class<? extends ExtendedNodeInfo> clazz) {
        LOGGER.trace("Registering extended node information type: {}", type);
        if (extendedNodeInfoTypes.put(type, clazz) != null) {
            throw new IllegalArgumentException("Extended node information type [" + type + "] is already registered");
        }
    }

    @Override
    protected void configure() {
        String extendedInfoType = settings.get(NODE_INFO_EXTENDED_TYPE, NODE_INFO_EXTENDED_DEFAULT_TYPE);
        LOGGER.trace("Using extended node information type: {}", extendedInfoType);

        Class<? extends ExtendedNodeInfo> extendedInfoClass = extendedNodeInfoTypes.get(extendedInfoType);
        if (extendedInfoClass == null) {
            throw new IllegalArgumentException("Unknown extended node information type [" + extendedInfoType + "]");
        }

        bind(ExtendedNodeInfo.class).to(extendedInfoClass).asEagerSingleton();
    }
}
