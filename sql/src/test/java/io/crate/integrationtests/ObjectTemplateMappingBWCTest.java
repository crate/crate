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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.Matchers.is;


@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0)
public class ObjectTemplateMappingBWCTest extends SQLTransportIntegrationTest {

    @Before
    public void loadLegacy() throws IOException {
        Path zippedIndexDir = getDataPath("/indices/bwc/bwc-object_template_mapping-0.54.9.zip");
        Settings nodeSettings = prepareBackwardsDataDir(zippedIndexDir);
        internalCluster().startNode(nodeSettings);
        ensureYellow();
    }

    /**
     * Test that the mapping of the template is updated and you can create new partitions with the mapping.
     */
    @Test
    public void testCreateNewPartition() throws IOException {
        execute("INSERT INTO object_template_mapping (ts, attr, month) VALUES (1480923560000, {temp=19.8}, '201612')");
        execute("REFRESH TABLE object_template_mapping");
        execute("SELECT COUNT(*) FROM object_template_mapping");
        assertThat((Long) response.rows()[0][0], is(2L));
        execute("SELECT partition_ident FROM information_schema.table_partitions WHERE table_name='object_template_mapping' ORDER BY 1");
        assertThat((String) response.rows()[0][0], is("043j4c1h6ooj2"));
        assertThat((String) response.rows()[1][0], is("043j4c1h6ooj4"));
    }

}
