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

package io.crate.integrationtests;

import io.crate.testing.TestingHelpers;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class BlobJoinIntegrationTest extends SQLHttpIntegrationTest {

    @Test
    public void testJoinWithBlobTable() throws Exception {
        execute("create blob table b1 with (number_of_replicas = 0)");
        execute("create blob table b2 with (number_of_replicas = 0)");
        ensureYellow();

        for (String content : new String[]{"foo", "bar", "baz"}) {
            upload("b1", content);
            upload("b2", content);
        }
        refresh();

        execute("select b1.digest from blob.b1 join blob.b2 on b1.digest = b2.digest");
        assertThat(TestingHelpers.printedTable(response.rows()),
            allOf(
                containsString("0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33\n"),
                containsString("62cdb7020ff920e5aa642c3d4066950dd1f01f4d\n"),
                containsString("bbe960a25ea311d21d40669e93df2003ba9b90a2\n")));
    }
}
