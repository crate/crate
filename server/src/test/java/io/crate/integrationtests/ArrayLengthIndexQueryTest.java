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

package io.crate.integrationtests;

import static io.crate.testing.Asserts.assertThat;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

@IntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0)
public class ArrayLengthIndexQueryTest extends IntegTestCase {

    /**
     * {@link TablesNeedUpgradeSysCheckTest#startUpNodeWithDataDir(String)}
     */
    private void startUpNodeWithDataDir(String dataPath) throws Exception {
        Path indexDir = createTempDir();
        try (InputStream stream = Files.newInputStream(getDataPath(dataPath))) {
            TestUtil.unzip(stream, indexDir);
        }
        Settings.Builder builder = Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), indexDir.toAbsolutePath());
        cluster().startNode(builder.build());
        ensureGreen();
    }

    // tracks a bug: support/issues/509
    @Test
    public void test_array_length_queries_are_not_affected_by_corrupted_version_settings_for_table_and_partitions() throws Exception {
        startUpNodeWithDataDir("/indices/data_home/cratedata-support-509.zip");
        execute("insert into doc.tbl values (4, {a=[1]})");
        execute("select * from doc.tbl where array_length(o['a'], 1) = 1");
        assertThat(response).hasRows(
            "1| {a=[1]}",
            "2| {a=[1]}",
            "3| {a=[1]}",
            "4| {a=[1]}"
        );

        /**
         *  To create '/indices/data_home/cratedata-support-509.zip':
         *
         * -- on 5.8:
         * cr> create table tbl (id int, o object as (a int[])) partitioned by (id);
         * cr> insert into tbl values (1, {a=[1]});
         * cr> select version['created'] from information_schema.table_partitions where table_name = 'tbl';
         * +--------------------+
         * | version['created'] |
         * +--------------------+
         * | 5.8.5              |
         * +--------------------+
         * cr> select values, version['created'] from information_schema.table_partitions where table_name = 'tbl';
         * +-----------+--------------------+
         * | values    | version['created'] |
         * +-----------+--------------------+
         * | {"id": 1} | 5.8.5              |
         * +-----------+--------------------+
         *
         *
         * -- upgraded to 5.9:
         * cr> insert into tbl values (2, {a=[1]});
         * cr> select version['created'] from information_schema.tables where table_name = 'tbl';
         * +--------------------+
         * | version['created'] |
         * +--------------------+
         * | 5.8.5              |
         * +--------------------+
         * SELECT 1 row in set (0.004 sec)
         * cr> select values, version['created'] from information_schema.table_partitions where table_name = 'tbl';
         * +-----------+--------------------+
         * | values    | version['created'] |
         * +-----------+--------------------+
         * | {"id": 2} | 5.8.5              |
         * | {"id": 1} | 5.8.5              |
         * +-----------+--------------------+
         * cr> alter table tbl set (refresh_interval = 2);
         * cr> select version['created'] from information_schema.tables where table_name = 'tbl';
         * +--------------------+
         * | version['created'] |
         * +--------------------+
         * | 5.9.7              | -- wrongly upgraded from 5.8.5
         * +--------------------+
         * cr> insert into tbl values (3, {a=[1]});
         *cr> select values, version['created'] from information_schema.table_partitions where table_name = 'tbl';
         * +-----------+--------------------+
         * | values    | version['created'] |
         * +-----------+--------------------+
         * | {"id": 3} | 5.9.7              | -- side effect of the wrong upgrade
         * | {"id": 2} | 5.8.5              |
         * | {"id": 1} | 5.8.5              |
         * +-----------+--------------------+
         */
    }
}
