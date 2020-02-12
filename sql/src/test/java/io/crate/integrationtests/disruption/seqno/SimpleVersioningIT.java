/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.crate.integrationtests.disruption.seqno;

import io.crate.integrationtests.SQLTransportIntegrationTest;
import org.junit.Test;


import static org.hamcrest.Matchers.equalTo;

public class SimpleVersioningIT extends SQLTransportIntegrationTest {

    @Test
    public void test_compare_and_set() {

        execute("create table test (id integer primary key, value string)");
        ensureGreen();

        execute("insert into test (id, value) values (?, ?) returning _seq_no, _primary_term", new Object[]{1, "value1_1"});

        long seqNo = (long) response.rows()[0][0];
        long primaryTerm = (long) response.rows()[0][1];

        assertThat(seqNo, equalTo(0L));
        assertThat(primaryTerm, equalTo(1L));

        execute("update test set value = 'value1_2' where id = 1 and _seq_no = 0 and _primary_term = 1 returning _seq_no, _primary_term");

        seqNo = (long) response.rows()[0][0];
        primaryTerm = (long) response.rows()[0][1];

        assertThat(seqNo, equalTo(1L));
        assertThat(primaryTerm, equalTo(1L));

        execute("update test set value = 'value1_1' where id = 1 and _seq_no = 10 and _primary_term = 1 returning _seq_no, _primary_term");
        assertThat(response.rowCount(), equalTo(0L));

        execute("update test set value = 'value1_1' where id = 1 and _seq_no = 10 and _primary_term = 2 returning _seq_no, _primary_term");
        assertThat(response.rowCount(), equalTo(0L));

        execute("update test set value = 'value1_1' where id = 1 and _seq_no = 1 and _primary_term = 2 returning _seq_no, _primary_term");
        assertThat(response.rowCount(), equalTo(0L));

        execute("delete from test where id = 1 and _seq_no = 10 and _primary_term = 1");
        assertThat(response.rowCount(), equalTo(0L));

        execute("delete from test where id = 1 and _seq_no = 10 and _primary_term = 2");
        assertThat(response.rowCount(), equalTo(0L));

        execute("delete from test where id = 1 and _seq_no = 1 and _primary_term = 2");
        assertThat(response.rowCount(), equalTo(0L));

        execute("refresh table test");

        for (int i = 0; i < 10; i++) {
            execute("select _seq_no, _primary_term from test where id = 1");
            assertThat(response.rows()[0][0], equalTo(1L));
            assertThat(response.rows()[0][1], equalTo(1L));
        }

        // select with versioning
        for (int i = 0; i < 10; i++) {
            execute("select _seq_no, _primary_term, _version from test");
            assertThat(response.rows()[0][0], equalTo(1L));
            assertThat(response.rows()[0][1], equalTo(1L));
            assertThat(response.rows()[0][2], equalTo(2L));
        }

        // select without versioning
        for (int i = 0; i < 10; i++) {
            execute("select _seq_no, _primary_term from test");
            assertThat(response.rows()[0][0], equalTo(1L));
            assertThat(response.rows()[0][1], equalTo(1L));
        }

        execute("delete from test where id = 1 and _seq_no = 1 and _primary_term = 1");
        assertThat(response.rowCount(), equalTo(1L));

        execute("delete from test where id = 1 and _seq_no = 2 and _primary_term = 1");
        assertThat(response.rowCount(), equalTo(0L));

    }

    public void test_simple_version_with_refresh() throws Exception {
        execute("create table test (id integer primary key, value string)");

        ensureGreen();

        execute("insert into test (id, value) values (?, ?) returning _seq_no", new Object[]{1, "value1_1"});

        long seqNo = (long) response.rows()[0][0];
        assertThat(seqNo, equalTo(0L));

        execute("refresh table test");

        execute("update test set value = 'value1_2' where id = 1 returning _seq_no, _primary_term");

        seqNo = (long) response.rows()[0][0];
        assertThat(seqNo, equalTo(1L));

        execute("refresh table test");

        execute("update test set value = 'value1_1' where id = 1 and _seq_no = 0 and _primary_term = 1 returning _seq_no, _primary_term");
        assertThat(response.rowCount(), equalTo(0L));

        execute("refresh table test");

        execute("delete from test where id = 1 and _seq_no = 0 and _primary_term = 1");
        assertThat(response.rowCount(), equalTo(0L));

        for (int i = 0; i < 10; i++) {
            execute("select _version from test where id=1");
            assertThat(response.rows()[0][0], equalTo(2L));
        }

        execute("refresh table test");

        for (int i = 0; i < 10; i++) {
            execute("select _version, _seq_no from test");
            assertThat(response.rowCount(), equalTo(1L));
            assertThat(response.rows()[0][0], equalTo(2L));
            assertThat(response.rows()[0][1], equalTo(1L));
        }
    }
}
