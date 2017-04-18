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

package org.elasticsearch.node;

import io.crate.Constants;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.transport.Netty3Plugin;
import org.elasticsearch.transport.TransportSettings;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class CrateSettingsPreparerTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testValidateKnownSettings() {
        Settings.Builder builder = Settings.builder()
            .put("stats.enabled", true)
            .put("psql.port", 5432);
        CrateSettingsPreparer.validateKnownSettings(builder);
    }

    @Test
    public void testValidateKnownIncorrectSettings() {
        Settings.Builder builder = Settings.builder()
            .put("stats.enabled", "foo")
            .put("psql.port", 5432);
        exception.expect(RuntimeException.class);
        exception.expectMessage(containsString("Invalid value [foo] for the [stats.enabled] setting."));
        CrateSettingsPreparer.validateKnownSettings(builder);
    }

    @Test
    public void testValidationOfUnknownSettingsMustBeIgnored() {
        Settings.Builder builder = Settings.builder()
            .put("path.home", true);
        try {
            CrateSettingsPreparer.validateKnownSettings(builder);
        } catch (Throwable e) {
            fail();
        }
    }

    @Test
    public void testDefaultCrateSettings() throws Exception {
        Settings.Builder builder = Settings.builder();
        InternalSettingsPreparer.initializeSettings(builder, Settings.EMPTY, Collections.emptyMap());
        InternalSettingsPreparer.finalizeSettings(builder, Terminal.DEFAULT);
        CrateSettingsPreparer.applyCrateDefaults(builder);

        assertThat(builder.get(NetworkModule.TRANSPORT_TYPE_DEFAULT_KEY), is(Netty3Plugin.NETTY_TRANSPORT_NAME));
        assertThat(builder.get(HttpTransportSettings.SETTING_HTTP_PORT.getKey()), is(Constants.HTTP_PORT_RANGE));
        assertThat(builder.get(TransportSettings.PORT.getKey()), is(Constants.TRANSPORT_PORT_RANGE));
        assertThat(builder.get(NetworkService.GLOBAL_NETWORK_HOST_SETTING.getKey()), is(NetworkService.DEFAULT_NETWORK_HOST));

        assertThat(builder.get(ClusterName.CLUSTER_NAME_SETTING.getKey()), is("crate"));
        assertThat(builder.get(Node.NODE_NAME_SETTING.getKey()), isIn(CrateSettingsPreparer.nodeNames()));
    }
}
