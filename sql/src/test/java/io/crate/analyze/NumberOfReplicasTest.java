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

package io.crate.analyze;

import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class NumberOfReplicasTest extends CrateUnitTest {

    @Test
    public void testFromEmptySettings() throws Exception {
        BytesRef numberOfResplicas = NumberOfReplicas.fromSettings(Settings.EMPTY);
        assertThat(numberOfResplicas.utf8ToString(), is("1"));
    }

    @Test
    public void testNumber() throws Exception {
        BytesRef numberOfResplicas = NumberOfReplicas.fromSettings(Settings.builder()
            .put(NumberOfReplicas.NUMBER_OF_REPLICAS, 4)
            .build());
        assertThat(numberOfResplicas.utf8ToString(), is("4"));
    }

    @Test
    public void testAutoExpandSettingsTakePrecedence() throws Exception {
        BytesRef numberOfResplicas = NumberOfReplicas.fromSettings(Settings.builder()
            .put(NumberOfReplicas.AUTO_EXPAND_REPLICAS, "0-all")
            .put(NumberOfReplicas.NUMBER_OF_REPLICAS, 1)
            .build());
        assertThat(numberOfResplicas.utf8ToString(), is("0-all"));
    }

    @Test
    public void testInvalidAutoExpandSettings() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The \"number_of_replicas\" range \"abc\" isn't valid");
        NumberOfReplicas.fromSettings(Settings.builder()
            .put(NumberOfReplicas.AUTO_EXPAND_REPLICAS, "abc")
            .put(NumberOfReplicas.NUMBER_OF_REPLICAS, 1)
            .build());
    }
}
