/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.module;

import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static org.cratedb.test.integration.PathAccessor.stringFromPath;

/**
 * Abstract base class for the plugin's rest action tests. Sets up the client
 * and delivers some base functionality needed for all tests.
 */
@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 1)
public abstract class AbstractRestActionTest extends CrateIntegrationTest {

    protected String node2;

    @Override
    public Settings indexSettings() {
        return ImmutableSettings.builder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .build();
    }

    @Before
    public void setUpInout() throws Exception {
        prepareCreate("users")
            .setSettings(ImmutableSettings.builder().loadFromClasspath("essetup/settings/test_b.json").build())
            .addMapping("d", stringFromPath("/essetup/mappings/test_b.json", getClass()))
            .execute().actionGet();

        loadBulk("/essetup/data/test_b.json", getClass());
        refresh();
    }

    @After
    public void cleanUpSecondNode() throws Exception {
        wipeIndices("_all");
        if (node2 != null) {
            cluster().stopNode(node2);
        }
    }

    /**
     * Convert an XContent object to a Java map
     * @param toXContent
     * @return
     * @throws IOException
     */
    public static Map<String, Object> toMap(ToXContent toXContent) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        toXContent.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return XContentFactory.xContent(XContentType.JSON).createParser(
                builder.string()).mapOrderedAndClose();
    }

    /**
     * Set up a second node and wait for green status
     */
    protected String setUpSecondNode() {
        node2 = cluster().startNode();
        waitForRelocation(ClusterHealthStatus.GREEN);
        return node2;
    }

    /**
     * Get a list of lines from a gzipped file.
     * Test fails if file not found or IO exception happens.
     *
     * @param filename the file name to read
     * @return a list of strings
     */
    protected List<String> readLinesFromGZIP(String filename) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(new FileInputStream(new File(filename)))));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            fail("File not found");
        } catch (IOException e) {
            e.printStackTrace();
            fail("IO Excsption while reading ZIP stream");
        }
        return readLines(filename, reader);
    }

    protected List<String> readLines(String filename, BufferedReader reader) {
        List<String> lines = new ArrayList<String>();
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
            fail("IO Exception occured while reading file");
        }
        return lines;
    }
}
