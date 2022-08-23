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

import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.assertj.core.api.Assertions.assertThat;

@IntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0)
public class IndexUpgraderTest extends IntegTestCase {

    /**
     * {@link TablesNeedUpgradeSysCheckTest#startUpNodeWithDataDir(String)}
     */
    private void startUpNodeWithDataDir(String dataPath) throws Exception {
        Path indexDir = createTempDir();
        try (InputStream stream = Files.newInputStream(getDataPath(dataPath))) {
            TestUtil.unzip(stream, indexDir);
        }
        Settings.Builder builder = Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), indexDir.toAbsolutePath());
        internalCluster().startNode(builder.build());
        ensureGreen();
    }

    @Test
    public void test_index_upgraders_fix_column_positions() throws Exception {
        startUpNodeWithDataDir("/indices/data_home/cratedata-5.0-bebe506.zip"); // https://github.com/crate/crate/commit/bebe5065dc6f6d50537919559bf1f0cfd3992be9
        /*      Table 1) 'testing'
                CREATE TABLE IF NOT EXISTS "doc"."testing" (
                "p1" text, -- position 1
                "nested" OBJECT(DYNAMIC) AS ( -- position 2
                  "sub1" OBJECT(DYNAMIC) AS ( -- position 3
                     "sub2" BIGINT, "sub3" bigint -- position 4, position 5
                    )
                ),
                "nested2" OBJECT(DYNAMIC) AS ( -- position 4
                      "sub1" OBJECT(DYNAMIC) AS ( -- position 5
                         "sub2" BIGINT, "sub3" bigint -- position 6, position 7
                        )
                   ),
                "obj" object(dynamic), -- position 6
                "obj2" object(dynamic) -- position 7
                );
                insert into testing (p1, obj) values (1, {a=1}); -- position 'null'


               Table 2) 'partitioned'
               CREATE TABLE IF NOT EXISTS "doc"."partitioned" (
               "p1" text, -- position 1
               "nested" OBJECT(DYNAMIC) AS ( -- position 2
                  "sub1" OBJECT(DYNAMIC) AS ( -- position 3
                     "sub2" BIGINT, "sub3" bigint -- position 4, position 5
                    )
               ),
               "nested2" OBJECT(DYNAMIC) AS ( -- position 4
                      "sub1" OBJECT(DYNAMIC) AS ( -- position 5
                         "sub2" BIGINT, "sub3" bigint -- position 6, position 7
                        )
                   ),
               "obj" object(dynamic), -- position 6
               "obj2" object(dynamic) -- position 7
               ) partitioned by (p1);
               insert into partitioned (p1, obj) values (1, {a=1}); -- position 'null'

               Notice the duplicates and 'null' columns.
               The only difference in the two are that one is partitioned and the other is not.
               The partitioned has template mapping and index mapping containing invalid column position whereas
               the unpartitioned has only index mapping to take care of.
         */

        // notice that duplicates and null positions are gone while keeping top-level positions (1, 2, 4, 6, 7)
        String expected = """
            nested| 2
            nested2| 4
            nested2['sub1']| 8
            nested2['sub1']['sub2']| 10
            nested2['sub1']['sub3']| 11
            nested['sub1']| 3
            nested['sub1']['sub2']| 12
            nested['sub1']['sub3']| 5
            obj| 6
            obj2| 7
            obj['a']| 9
            p1| 1
            """;
        execute("select column_name, ordinal_position from information_schema.columns where table_name = 'partitioned' order by 1");
        assertThat(printedTable(response.rows())).isEqualTo(expected);

        execute("select column_name, ordinal_position from information_schema.columns where table_name = 'testing' order by 1");
        assertThat(printedTable(response.rows())).isEqualTo(expected);
    }
}
