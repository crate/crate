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

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;

import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;


@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SysSegmentsTableInfoTest extends SQLIntegrationTestCase {

    @Test
    public void test_retrieve_segment_information() {
        execute("create table t1 (id INTEGER, name STRING) clustered into 1 shards" +
                " partitioned by(id)" +
                " with (number_of_replicas = 1)");
        execute("insert into t1 values (1, 'test')");
        refresh();
        ensureGreen();
        execute("select table_name, shard_id, segment_name, generation, num_docs, deleted_docs, size, memory, " +
                "committed, search, compound, version, node, node['id'], node['name'], attributes, primary, partition_ident" +
                " from sys.segments order by primary desc");
        assertThat(response.rowCount(), is(2L));
        validateResponse(response.rows()[0], true);
        validateResponse(response.rows()[1], false);
    }

    @SuppressWarnings("unchecked")
    private void validateResponse(Object[] result, boolean primary) {
        assertThat(result[0], is("t1"));
        assertThat(result[1], is(0));
        assertThat(result[2], is(notNullValue()));
        assertThat(result[3], is(0L));
        assertThat(result[4], is(1));
        assertThat(result[5], is(0));
        assertThat((Long) result[6], greaterThan(2700L));
        assertThat((Long) result[7], greaterThan(900L));
        assertThat(result[8], anyOf(is(true), is(false)));
        assertThat(result[9], is(true));
        assertThat(result[10], is(true));
        assertThat(result[11], is(Version.CURRENT.luceneVersion.toString()));
        Map<String, Object> node = (Map<String, Object>) result[12];
        assertNotNull(node);
        assertThat(node.size(), is(2));
        assertThat(node.keySet(), containsInAnyOrder("name", "id"));
        assertNotNull(result[13]);
        assertNotNull(result[14]);
        Map attributes = (Map) result[15];
        assertNotNull(attributes);
        assertThat(attributes.size(), is(1));
        assertThat(result[16], is(primary));
        assertThat(result[17], is("04132"));
    }
}
