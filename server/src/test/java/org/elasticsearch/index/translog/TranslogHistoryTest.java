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

package org.elasticsearch.index.translog;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.test.IntegTestCase;


@IntegTestCase.ClusterScope(numDataNodes = 2)
public class TranslogHistoryTest extends IntegTestCase {

    public void test_translog_is_trimmed_with_soft_deletes_enabled() throws Exception {
        execute("create table doc.test(x int) clustered into 1 shards with(number_of_replicas=1, \"soft_deletes.enabled\"='true')");
        ensureGreen();

        int numDocs = randomIntBetween(1, 10);
        for(int i = 0; i < numDocs; i++) {
            execute("insert into doc.test(x) values(?)", new Object[]{i});
        }

        execute("refresh table doc.test");
        execute("optimize table doc.test");

        ensureGreen();

        assertBusy(() -> {
            execute("select translog_stats['number_of_operations'] from sys.shards where table_name='test' and primary=true");
            assertThat(response.rows()[0][0]).isEqualTo(0);
        });
        assertBusy(() -> {
            execute("select translog_stats['number_of_operations'] from sys.shards where table_name='test' and primary=false");
            assertThat(response.rows()[0][0]).isEqualTo(0);
        });
    }
}
