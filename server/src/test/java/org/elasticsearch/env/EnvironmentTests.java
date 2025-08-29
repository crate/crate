/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.env;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class EnvironmentTests extends ESTestCase {

    @Test
    public void testRelativePaths() {
        Path cwd = Paths.get(System.getProperty("user.dir"));
        String home = "home/";
        Path config = Paths.get("./config");
        String[] data = new String[] {"../data1", "/data2"};
        String logs = "../../logs";
        String repo = "../repo/";
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), home)
            .putList(Environment.PATH_DATA_SETTING.getKey(), data)
            .put(Environment.PATH_LOGS_SETTING.getKey(), logs)
            .put(Environment.PATH_REPO_SETTING.getKey(), repo)
            .build();

        Environment env = new Environment(settings, config);
        assertThat(env.configFile()).hasToString(Paths.get(cwd.toString(), "home/config").toString());
        assertThat(env.logsFile()).hasToString(Paths.get(cwd.toString(), "../logs").toString());
        assertThat(env.dataFiles()).satisfiesExactly(
            d -> assertThat(d).hasToString(Paths.get(cwd.toString(), "data1").toString()),
            d -> assertThat(d).hasToString(Paths.get("/data2").toString())
        );
        assertThat(env.repoFiles()).satisfiesExactly(
            r -> assertThat(r).hasToString(Paths.get(cwd.toString(), "repo").toString())
        );
    }
}
