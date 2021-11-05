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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_READ_POLL_DURATION;
import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.is;

public class MetadataTrackerITest extends LogicalReplicationIntegrationTest {

    @Override
    Settings logicalReplicationSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put(super.logicalReplicationSettings());
        // Increase poll duration to 1s to make sure there is an out-of-sync situation
        // when the mapping changes on the subscriber cluster
        builder.put(REPLICATION_READ_POLL_DURATION.getKey(), "1s");
        return builder.build();
    }

    @Test
    @TestLogging("_root:INFO,io.crate.replication.logical:DEBUG")
    public void test_subscribed_tables_are_followed_when_schema_changed() throws Exception {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT) CLUSTERED INTO 1 SHARDS WITH(" +
                           " number_of_replicas=0," +
                           " \"translog.flush_threshold_size\"='64b'" +
                           ")");
        executeOnPublisher("INSERT INTO doc.t1 (id) VALUES (1), (2)");

        createPublication("pub1", false, List.of("doc.t1"));

        createSubscription("sub1", "pub1");

        assertBusy(() -> {
            var response = executeOnSubscriber("SELECT * FROM doc.t1");
            assertThat(printedTable(response.rows()), is("1\n" +
                                                         "2\n"));
        }, 10, TimeUnit.SECONDS);

        executeOnPublisher("ALTER TABLE doc.t1 ADD COLUMN value string");
        executeOnPublisher("REFRESH TABLE doc.t1");

        assertBusy(() -> {
            executeOnSubscriber("REFRESH TABLE doc.t1");
            var r = executeOnSubscriber("SELECT * FROM doc.t1");
            assertThat(printedTable(r.rows()), is("1| NULL\n" +
                                                  "2| NULL\n"));

        }, 10, TimeUnit.SECONDS);
    }


    @Test
    @TestLogging("_root:INFO,io.crate.replication.logical:DEBUG")
    public void test_subscribed_tables_are_followed_when_schema_changed_and_new_data_is_synced() throws Exception {
        executeOnPublisher("CREATE TABLE doc.t1 (id INT) CLUSTERED INTO 1 SHARDS WITH(" +
                           " number_of_replicas=0," +
                           " \"translog.flush_threshold_size\"='64b'" +
                           ")");
        executeOnPublisher("INSERT INTO doc.t1 (id) VALUES (1), (2)");

        createPublication("pub1", false, List.of("doc.t1"));

        createSubscription("sub1", "pub1");

        executeOnPublisher("ALTER TABLE doc.t1 ADD COLUMN name varchar");
        //This insert is synced to the subscriber and the mapping might not be updated yet
        executeOnPublisher("INSERT INTO doc.t1 (id, name) VALUES (3, 'chewbacca')");
        executeOnPublisher("INSERT INTO doc.t1 (id, name) VALUES (4, 'r2d2')");
        executeOnPublisher("REFRESH TABLE doc.t1");
        //Lets alter the table again
        executeOnPublisher("ALTER TABLE doc.t1 ADD COLUMN age integer");
        executeOnPublisher("INSERT INTO doc.t1 (id, name, age) VALUES (5, 'luke', 37)");
        executeOnPublisher("INSERT INTO doc.t1 (id, name, age) VALUES (6, 'yoda', 900)");
        executeOnPublisher("REFRESH TABLE doc.t1");

        assertBusy(() -> {
            executeOnSubscriber("REFRESH TABLE doc.t1");
            var r = executeOnSubscriber("SELECT * FROM doc.t1");
            assertThat(printedTable(r.rows()), is("1| NULL| NULL\n" +
                                                  "2| NULL| NULL\n" +
                                                  "3| chewbacca| NULL\n"+
                                                  "4| r2d2| NULL\n" +
                                                  "5| luke| 37\n" +
                                                  "6| yoda| 900\n"));

        }, 10, TimeUnit.SECONDS);

    }

    @Test
    @TestLogging("_root:INFO,io.crate.replication.logical:DEBUG")
    public void test_add_multiple_tables() throws Exception {
        int numberOfTables = randomIntBetween(1,10);
        var tableNames = new ArrayList<String>();
        for (int i = 1; i <= numberOfTables; i++) {
            tableNames.add("doc.t" + i);
        }

        for (String tableName : tableNames) {
            executeOnPublisher("CREATE TABLE " + tableName + " (id INT) CLUSTERED INTO 1 SHARDS WITH(" +
                               " number_of_replicas=0," +
                               " \"translog.flush_threshold_size\"='64b'" +
                               ")");
            executeOnPublisher("INSERT INTO " + tableName + " (id) VALUES (1), (2)");
        }

        createPublication("pub1", false, tableNames);

        createSubscription("sub1", "pub1");

        for (String tableName : tableNames) {
            executeOnPublisher("ALTER TABLE " + tableName + " ADD COLUMN name varchar");
            executeOnPublisher("INSERT INTO " + tableName + " (id, name) VALUES (3, 'chewbacca'), (4, 'r2d2')");
        }

        executeOnPublisher("REFRESH TABLE " + String.join(",", tableNames));

        for (String tableName : tableNames) {
            executeOnPublisher("ALTER TABLE " + tableName + " ADD COLUMN age integer");
        }

        for (String tableName : tableNames) {
            executeOnPublisher("INSERT INTO " + tableName + "(id, name, age) VALUES (5, 'luke', 37), (6, 'yoda', 900)");
        }

        assertBusy(() -> {
            executeOnSubscriber("REFRESH TABLE " + String.join(",", tableNames));
            for (String tableName : tableNames) {
                var r = executeOnSubscriber("SELECT * FROM " + tableName);
                assertThat(printedTable(r.rows()), is("1| NULL| NULL\n" +
                                                      "2| NULL| NULL\n" +
                                                      "3| chewbacca| NULL\n" +
                                                      "4| r2d2| NULL\n" +
                                                      "5| luke| 37\n" +
                                                      "6| yoda| 900\n"));
            }
        }, 10, TimeUnit.SECONDS);
    }
}
