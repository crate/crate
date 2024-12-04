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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;


@IntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SysSegmentsTableInfoTest extends IntegTestCase {

    @Test
    public void test_retrieve_segment_information() {
        execute(
            """
                CREATE TABLE t1 (id INTEGER, name STRING)
                CLUSTERED INTO 1 SHARDS
                PARTITIONED BY(id)
                WITH (number_of_replicas = 1)
            """);
        execute("INSERT INTO t1 VALUES(1, 'test')");
        execute("refresh table t1");
        ensureGreen();
        execute(
            """
                SELECT table_name, shard_id, segment_name, generation, num_docs, deleted_docs, size, memory, committed,
                       search, compound, version, node, node['id'], node['name'], attributes, primary, partition_ident
                FROM sys.segments ORDER BY primary DESC
            """);
        assertThat(response.rowCount()).isEqualTo(2);
        validateResponse(response.rows()[0], true);
        validateResponse(response.rows()[1], false);
    }

    @SuppressWarnings("unchecked")
    private void validateResponse(Object[] result, boolean primary) {
        assertThat(result[0]).isEqualTo("t1");
        assertThat(result[1]).isEqualTo(0);
        assertThat(result[2]).isNotNull();
        assertThat((Long) result[3]).isNotNegative();
        assertThat(result[4]).isEqualTo(1);
        assertThat(result[5]).isEqualTo(0);
        assertThat((Long) result[6]).isGreaterThan(2700L);
        assertThat((Long) result[7]).isEqualTo(-1L);
        assertThat(result[8]).isIn(true, false);
        assertThat(result[9]).isEqualTo(true);
        assertThat(result[10]).isEqualTo(true);
        assertThat(result[11]).isEqualTo(Version.CURRENT.luceneVersion.toString());
        Map<String, Object> node = (Map<String, Object>) result[12];
        assertThat(node).isNotNull();
        assertThat(node).hasSize(2);
        assertThat(node.keySet()).containsExactlyInAnyOrder("name", "id");
        assertThat(result[13]).isNotNull();
        assertThat(result[14]).isNotNull();
        Map<?, ?> attributes = (Map<?, ?>) result[15];
        assertThat(attributes).isNotNull();
        assertThat(attributes).hasSize(1);
        assertThat(result[16]).isEqualTo(primary);
        assertThat(result[17]).isEqualTo("04132");
    }
}
