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
import io.crate.common.unit.TimeValue;
import org.elasticsearch.gateway.GatewayService;

@Singleton
public class RecoveryAfterTimeSysCheck extends AbstractSysNodeCheck {

    private final Settings settings;

    static final int ID = 3;
    private static final String DESCRIPTION = "If any of the \"expected nodes\" recovery settings are set, the value of 'gateway.recover_after_time' " +
                                              "must not be zero. Otherwise the \"expected nodes\" setting wouldn't have any effect.";

    @Inject
    public RecoveryAfterTimeSysCheck(Settings settings) {
        super(ID, DESCRIPTION, Severity.MEDIUM);
        this.settings = settings;
    }

    @Override
    public boolean isValid() {
        return validate(
            GatewayService.RECOVER_AFTER_TIME_SETTING.get(settings),
            GatewayService.RECOVER_AFTER_NODES_SETTING.get(settings),
            GatewayService.EXPECTED_NODES_SETTING.get(settings)
        );
    }

    protected boolean validate(TimeValue recoverAfterTime, int recoveryAfterNodes, int expectedNodes) {
        return recoveryAfterNodes <= expectedNodes &&
               (expectedNodes == GatewayService.EXPECTED_NODES_SETTING.getDefault(Settings.EMPTY) ||
                recoverAfterTime.getMillis() > 0L);
    }
}
