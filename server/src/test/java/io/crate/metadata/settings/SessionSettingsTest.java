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

package io.crate.metadata.settings;


import io.crate.metadata.SearchPath;
import io.crate.planner.optimizer.rule.MergeFilters;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class SessionSettingsTest {

    @Test
    public void testSessionSettingsStreaming() throws IOException {
        SessionSettings s1 = new SessionSettings("user", SearchPath.createSearchPathFrom("crate"), true, Set.of(
            MergeFilters.class), false);
        BytesStreamOutput out = new BytesStreamOutput();
        s1.writeTo(out);

        SessionSettings s2 = new SessionSettings(out.bytes().streamInput());
        assertEquals(s1, s2);
    }

    @Test
    public void testSessionSettingsStreamingFrom4_6_0() throws IOException {
        SessionSettings s1 = new SessionSettings("user", SearchPath.createSearchPathFrom("crate"), true, Set.of(
            MergeFilters.class), false);
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_4_6_0);
        s1.writeTo(out);
        var in = out.bytes().streamInput();
        in.setVersion(Version.V_4_6_0);
        SessionSettings actual = new SessionSettings(in);
        SessionSettings expected = new SessionSettings("user", SearchPath.createSearchPathFrom("crate"), true, Set.of(
            MergeFilters.class), true);
        assertEquals(expected, actual);
    }
}
