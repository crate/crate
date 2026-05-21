/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package org.elasticsearch.action.admin.cluster.repositories.put;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class PutRepositoryRequestTest extends ESTestCase {

    @Test
    public void test_streaming() throws Exception {
        PutRepositoryRequest request = new PutRepositoryRequest(
            "dummy_repo",
            "dummy_type",
            Settings.builder().put("foo", "bar").build()
        );


        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        PutRepositoryRequest actual = new PutRepositoryRequest(out.bytes().streamInput());
        assertThat(actual.name()).isEqualTo(request.name());
        assertThat(actual.type()).isEqualTo(request.type());
        assertThat(actual.settings()).isEqualTo(request.settings());

        out = new BytesStreamOutput();
        out.setVersion(Version.V_6_3_2);
        request.writeTo(out);
        var in = out.bytes().streamInput();
        in.setVersion(Version.V_6_3_2);
        actual = new PutRepositoryRequest(in);
        assertThat(actual.name()).isEqualTo(request.name());
        assertThat(actual.type()).isEqualTo(request.type());
        assertThat(actual.settings()).isEqualTo(request.settings());
    }
}
