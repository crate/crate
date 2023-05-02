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

package io.crate.expression.reference.sys.check.node;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;

import io.crate.common.unit.TimeValue;

@Singleton
public class RecoveryAfterTimeSysCheck extends AbstractSysNodeCheck {

    private final Settings settings;

    static final int ID = 3;
    private static final String DESCRIPTION = "If any of the \"expected data nodes\" recovery settings are set " +
                                              "(or the deprecated \"expected nodes\" settings)," +
                                              "the value of 'gateway.recover_after_time' must not be zero. Otherwise " +
                                              "the \"expected (data) nodes\" setting wouldn't have any effect.";

    @Inject
    public RecoveryAfterTimeSysCheck(Settings settings) {
        super(ID, DESCRIPTION, Severity.MEDIUM);
        this.settings = settings;
    }

    @Override
    public boolean isValid() {
        int afterNodes = GatewayService.RECOVER_AFTER_DATA_NODES_SETTING.get(settings);
        int expectedNodes = GatewayService.EXPECTED_DATA_NODES_SETTING.get(settings);
        int expectedNodesDefaultValue = GatewayService.EXPECTED_DATA_NODES_SETTING.getDefault(Settings.EMPTY);
        if (afterNodes == -1 || expectedNodes == expectedNodesDefaultValue) {
            // fallback to deprecated settings for BWC
            afterNodes = GatewayService.RECOVER_AFTER_NODES_SETTING.get(settings);
            expectedNodes = GatewayService.EXPECTED_NODES_SETTING.get(settings);
            expectedNodesDefaultValue = GatewayService.EXPECTED_NODES_SETTING.getDefault(Settings.EMPTY);
        }
        return validate(
            GatewayService.RECOVER_AFTER_TIME_SETTING.get(settings),
            afterNodes,
            expectedNodes,
            expectedNodesDefaultValue
        );
    }

    private static boolean validate(TimeValue recoverAfterTime,
                                    int recoveryAfterNodes,
                                    int expectedNodes,
                                    int expectedNodesDefaultValue) {
        return recoveryAfterNodes <= expectedNodes &&
               (expectedNodes == expectedNodesDefaultValue ||
                recoverAfterTime.millis() > 0L);
    }
}
