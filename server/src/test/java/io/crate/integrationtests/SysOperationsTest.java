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

package io.crate.integrationtests;

import static io.crate.testing.Asserts.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.SQLResponse;

public class SysOperationsTest extends IntegTestCase {

    @Test
    public void testDistinctSysOperations() throws Exception {
        // this tests a distributing collect without shards but DOC level granularity
        SQLResponse response = execute("select distinct name  from sys.operations limit 1");
        assertThat(response).hasRowCount(1);
    }

    @Test
    public void testQueryNameFromSysOperations() throws Exception {
        SQLResponse resp = execute("select name, job_id from sys.operations order by name asc");

        // usually this should return collect per node and an optional merge on handler
        // but it could be that the collect is finished before the localMerge task is started in which
        // case it is missing.

        assertThat(resp.rowCount()).isGreaterThanOrEqualTo(cluster().numDataNodes());
        List<String> names = new ArrayList<>();
        for (Object[] objects : resp.rows()) {
            names.add((String) objects[0]);
        }
        Collections.sort(names);
        assertThat(names.contains("collect")).isTrue();
    }

    @Test
    public void testNodeExpressionOnSysOperations() throws Exception {
        execute("select * from sys.nodes");
        SQLResponse response = execute("select node['name'], id from sys.operations limit 1");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0].toString()).startsWith("node_s");
    }
}
