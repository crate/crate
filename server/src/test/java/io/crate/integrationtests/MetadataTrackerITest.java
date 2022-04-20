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

import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.MetadataTracker;
import io.crate.testing.UseRandomizedSchema;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.is;

@UseRandomizedSchema(random = false)
public class MetadataTrackerITest extends LogicalReplicationITestCase {

    @Test
    public void test_schema_changes_of_subscribed_table_is_replicated() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" + defaultTableSettings() +")");
        executeOnPublisher("INSERT INTO t1 (id) VALUES (1), (2)");
        createPublication("pub1", false, List.of("t1"));
        createSubscription("sub1", "pub1");

        // Ensure tracker has started
        assertBusy(() -> assertThat(isTrackerActive(), is(true)));

        executeOnPublisher("ALTER TABLE t1 ADD COLUMN value string");
        assertBusy(() -> {
            var r = executeOnSubscriber("SELECT column_name FROM information_schema.columns" +
                                        " WHERE table_name = 't1'" +
                                        " ORDER BY ordinal_position");
            assertThat(printedTable(r.rows()), is("id\n" +
                                                  "value\n"));
        });
    }

    @Test
    public void test_schema_changes_of_subscribed_table_is_replicated_and_new_data_is_synced() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" + defaultTableSettings() +")");
        executeOnPublisher("INSERT INTO t1 (id) VALUES (1), (2)");
        createPublication("pub1", false, List.of("t1"));
        createSubscription("sub1", "pub1");

        // Ensure tracker has started
        assertBusy(() -> assertThat(isTrackerActive(), is(true)));

        executeOnPublisher("ALTER TABLE t1 ADD COLUMN name varchar");
        //This insert is synced to the subscriber and the mapping might not be updated yet
        executeOnPublisher("INSERT INTO t1 (id, name) VALUES (3, 'chewbacca')");
        executeOnPublisher("INSERT INTO t1 (id, name) VALUES (4, 'r2d2')");
        executeOnPublisher("REFRESH TABLE t1");
        //Lets alter the table again
        executeOnPublisher("ALTER TABLE t1 ADD COLUMN age integer");
        executeOnPublisher("INSERT INTO t1 (id, name, age) VALUES (5, 'luke', 37)");
        executeOnPublisher("INSERT INTO t1 (id, name, age) VALUES (6, 'yoda', 900)");
        executeOnPublisher("REFRESH TABLE t1");

        assertBusy(() -> {
            executeOnSubscriber("REFRESH TABLE t1");
            var r = executeOnSubscriber("SELECT * FROM t1 ORDER BY id");
            assertThat(printedTable(r.rows()), is("1| NULL| NULL\n" +
                                                  "2| NULL| NULL\n" +
                                                  "3| chewbacca| NULL\n"+
                                                  "4| r2d2| NULL\n" +
                                                  "5| luke| 37\n" +
                                                  "6| yoda| 900\n"));

        });
    }

    @Test
    public void test_schema_changes_of_multiple_tables_are_replicated() throws Exception {
        int numberOfTables = randomIntBetween(1,10);
        var tableNames = new ArrayList<String>();
        for (int i = 1; i <= numberOfTables; i++) {
            executeOnPublisher("CREATE TABLE t" + i + " (id INT) WITH(" + defaultTableSettings() +")");
            tableNames.add("t" + i);
        }

        createPublication("pub1", false, tableNames);
        createSubscription("sub1", "pub1");

        // Ensure tracker has started
        assertBusy(() -> assertThat(isTrackerActive(), is(true)));

        for (String tableName : tableNames) {
            executeOnPublisher("ALTER TABLE " + tableName + " ADD COLUMN name varchar");
        }

        assertBusy(() -> {
            for (String tableName : tableNames) {
                var r = executeOnSubscriber("SELECT column_name FROM information_schema.columns" +
                                            " WHERE table_name = '" + tableName + "'" +
                                            " ORDER BY ordinal_position");
                assertThat(printedTable(r.rows()), is("id\n" +
                                                      "name\n"));
            }
        });
    }

    @Test
    public void test_new_table_and_new_partition_is_replicated() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT)");
        executeOnPublisher("CREATE TABLE t2 (id INT, p INT) PARTITIONED BY (p)");
        createPublication("pub1", true, List.of("t1", "t2"));
        createSubscription("sub1", "pub1");

        // Ensure tracker has started
        assertBusy(() -> assertThat(isTrackerActive(), is(true)));

        // Create new partition
        executeOnPublisher("INSERT INTO t2 (id, p) VALUES (1, 1)");
        // Create new table
        executeOnPublisher("CREATE TABLE t3 (id INT)");
        executeOnPublisher("GRANT DQL ON TABLE t3 TO " + SUBSCRIBING_USER);

        assertBusy(() -> {
            var r = executeOnSubscriber("SELECT column_name FROM information_schema.columns" +
                                        " WHERE table_name = 't3'" +
                                        " ORDER BY ordinal_position");
            assertThat(printedTable(r.rows()), is("id\n"));
            r = executeOnSubscriber("SELECT values FROM information_schema.table_partitions" +
                                        " WHERE table_name = 't2'" +
                                        " ORDER BY partition_ident");
            assertThat(printedTable(r.rows()), is("{p=1}\n"));
            ensureGreenOnSubscriber();
        }, 50, TimeUnit.SECONDS);
    }

    @Test
    public void test_new_empty_partitioned_table_is_replicated() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT)");
        createPublication("pub1", true, List.of("t1"));
        createSubscription("sub1", "pub1");

        // Ensure tracker has started
        assertBusy(() -> assertThat(isTrackerActive(), is(true)));

        // Create new table
        executeOnPublisher("CREATE TABLE t2 (id INT, p INT) PARTITIONED BY (p)");
        executeOnPublisher("GRANT DQL ON TABLE t2 TO " + SUBSCRIBING_USER);

        assertBusy(() -> {
            var r = executeOnSubscriber("SELECT column_name FROM information_schema.columns" +
                                        " WHERE table_name = 't2'" +
                                        " ORDER BY ordinal_position");
            assertThat(printedTable(r.rows()), is("id\n" +
                                                  "p\n"));
        });
    }

    @Test
    public void test_deleted_partition_is_replicated() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT, p INT) PARTITIONED BY (p)");
        executeOnPublisher("INSERT INTO t1 (id, p) VALUES (1, 1), (2, 2)");
        createPublication("pub1", true, List.of("t1"));
        createSubscription("sub1", "pub1");

        // Ensure tracker has started
        assertBusy(() -> assertThat(isTrackerActive(), is(true)));

        // Wait until table is replicated
        assertBusy(() -> {
            executeOnSubscriber("REFRESH TABLE t1");
            var r = executeOnSubscriber("SELECT id, p FROM t1 ORDER BY id");
            assertThat(printedTable(r.rows()), is(
                "1| 1\n" +
                "2| 2\n"));

        });

        // Drop partition
        executeOnPublisher("DELETE FROM t1 WHERE p = 1");

        assertBusy(() -> {
            executeOnSubscriber("REFRESH TABLE t1");
            var r = executeOnSubscriber("SELECT id, p FROM t1 ORDER BY id");
            assertThat(printedTable(r.rows()), is("2| 2\n"));
        });
    }

    @Test
    public void test_deleted_and_recreated_partition_is_also_deleted_and_restored_on_subscriber() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT, p INT) PARTITIONED BY (p)");
        executeOnPublisher("INSERT INTO t1 (id, p) VALUES (1, 1), (2, 2)");
        createPublication("pub1", true, List.of("t1"));
        createSubscription("sub1", "pub1");

        // Ensure tracker has started
        assertBusy(() -> assertThat(isTrackerActive(), is(true)));

        // Wait until table is replicated
        assertBusy(() -> {
            executeOnSubscriber("REFRESH TABLE t1");
            var r = executeOnSubscriber("SELECT id, p FROM t1 ORDER BY id");
            assertThat(printedTable(r.rows()), is(
                "1| 1\n" +
                "2| 2\n"));
        });

        // Drop partition
        executeOnPublisher("DELETE FROM t1 WHERE p = 1");
        // Re-create same partition with different value
        executeOnPublisher("INSERT INTO t1 (id, p) VALUES (11, 1)");

        assertBusy(() -> {
            executeOnSubscriber("REFRESH TABLE t1");
            var r = executeOnSubscriber("SELECT id, p FROM t1 ORDER BY id");
            assertThat(printedTable(r.rows()), is(
                "2| 2\n" +
                    "11| 1\n"));        // <- this must contain the id of the re-created partition
        }, 50, TimeUnit.SECONDS);
    }

    @Test
    public void test_dropped_partitioned_table_is_replicated() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT, p INT) PARTITIONED BY (p)");
        executeOnPublisher("INSERT INTO t1 (id, p) VALUES (1, 1), (2, 2)");
        createPublication("pub1", true, List.of("t1"));
        createSubscription("sub1", "pub1");

        // Ensure tracker has started
        assertBusy(() -> assertThat(isTrackerActive(), is(true)));

        // Wait until table is replicated
        assertBusy(() -> {
            executeOnSubscriber("REFRESH TABLE t1");
            var r = executeOnSubscriber("SELECT id, p FROM t1 ORDER BY id");
            assertThat(printedTable(r.rows()), is(
                "1| 1\n" +
                    "2| 2\n"));

        });

        // Drop table
        executeOnPublisher("DROP TABLE t1");

        assertBusy(() -> {
            var r = executeOnSubscriber("SELECT column_name FROM information_schema.columns" +
                " WHERE table_name = 't1'" +
                " ORDER BY ordinal_position");
            assertThat(r.rowCount(), is(0L));
        });
    }

    @Test
    public void test_dropped_table_is_replicated() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT)");
        executeOnPublisher("INSERT INTO t1 (id) VALUES (1), (2)");
        createPublication("pub1", true, List.of("t1"));
        createSubscription("sub1", "pub1");

        // Ensure tracker has started
        assertBusy(() -> assertThat(isTrackerActive(), is(true)));

        // Wait until table is replicated
        assertBusy(() -> {
            executeOnSubscriber("REFRESH TABLE t1");
            var r = executeOnSubscriber("SELECT id FROM t1 ORDER BY id");
            assertThat(printedTable(r.rows()), is(
                "1\n" +
                "2\n"));

        });

        // Drop table
        executeOnPublisher("DROP TABLE t1");

        assertBusy(() -> {
            var r = executeOnSubscriber("SELECT column_name FROM information_schema.columns" +
                " WHERE table_name = 't1'" +
                " ORDER BY ordinal_position");
            assertThat(r.rowCount(), is(0L));
        });
    }

    @Test
    public void test_drop_publication_stops_the_replication_on_subscriber() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT)");
        executeOnPublisher("INSERT INTO t1 (id) VALUES (1), (2)");
        createPublication("pub1", true, List.of("t1"));
        createSubscription("sub1", "pub1");

        assertBusy(() -> assertThat(isTrackerActive(), is(true)));
        var response = executeOnPublisher("SELECT * FROM pg_publication WHERE pubname = 'pub1'");
        assertThat(response.rowCount(), CoreMatchers.is(1L));

        executeOnPublisher("DROP PUBLICATION pub1");
        assertBusy(() -> assertThat(isTrackerActive(), is(false)));
        assertBusy(
            () -> {
                var res = executeOnSubscriber(
                    "SELECT s.subname, sr.srrelid::text, sr.srsubstate, sr.srsubstate_reason" +
                        " FROM pg_subscription s" +
                        " LEFT JOIN pg_subscription_rel sr ON s.oid = sr.srsubid" +
                        " ORDER BY s.subname");
                assertThat(printedTable(res.rows()), CoreMatchers.is("sub1| doc.t1| e| Tracking of metadata failed for subscription 'sub1' with unrecoverable error, stop tracking.\n" +
                    "Reason: Publication 'pub1' unknown\n"));
            }, 20, TimeUnit.SECONDS
        );

        assertThrowsMatches(
            () -> executeOnSubscriber("INSERT INTO t1 (id) VALUES(3)"),
            OperationOnInaccessibleRelationException.class,
            "The relation \"doc.t1\" doesn't allow INSERT operations, because it is included in a logical replication."
        );

        executeOnSubscriber("DROP SUBSCRIPTION sub1");

        response = executeOnSubscriber("INSERT INTO t1 (id) VALUES(3)");
        assertThat(response.rowCount(), CoreMatchers.is(1L));
    }

    @Test
    public void test_alter_publication_drop_table_tracking_is_alive_new_records_are_invisible_on_subscriber() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" + defaultTableSettings() + ")");
        executeOnPublisher("CREATE TABLE t2 (id INT, p INT) PARTITIONED BY (p) WITH(" + defaultTableSettings() + ")");
        executeOnPublisher("INSERT INTO t1 (id) VALUES (1), (2)");
        executeOnPublisher("INSERT INTO t2 (id, p) VALUES (1, 1), (2, 2)");
        createPublication("pub1", false, List.of("t1", "t2"));
        createSubscription("sub1", "pub1");

        // t2 will be dropped from the publication but current state is visible
        assertBusy(
            () -> {
                var res = executeOnSubscriber(
                    "SELECT s.subname, sr.srrelid::text, sr.srsubstate, sr.srsubstate_reason" +
                        " FROM pg_subscription s" +
                        " LEFT JOIN pg_subscription_rel sr ON s.oid = sr.srsubid" +
                        " ORDER BY s.subname");
                assertThat(res.rowCount(), Matchers.is(2L));

                var r = executeOnSubscriber("SELECT id FROM t2 ORDER BY id");
                assertThat(printedTable(r.rows()), Matchers.is(
                    "1\n" +
                        "2\n"));
            }, 20, TimeUnit.SECONDS
        );

        executeOnPublisher("ALTER PUBLICATION pub1 DROP TABLE t1, t2");

        // adding new records on the publisher to the tables excluded from the publication.
        executeOnPublisher("INSERT INTO t1 (id) VALUES (3)");
        executeOnPublisher("INSERT INTO t2 (id, p) VALUES (3, 3)");

        assertBusy(() -> assertThat(isTrackerActive(), is(true)));

        assertBusy(
            () -> {
                var res = executeOnSubscriber("SELECT id FROM t1 ORDER BY id");
                assertThat(printedTable(res.rows()), Matchers.is(
                    "1\n" +
                        "2\n")); // Subscriber doesn't see new record (3).

                res = executeOnSubscriber("SELECT id, p FROM t2 ORDER BY id");
                assertThat(printedTable(res.rows()), Matchers.is(
                    "1| 1\n" +
                        "2| 2\n"));  // Subscriber doesn't see new record (3,3).
            }, 20, TimeUnit.SECONDS
        );

//        assertThrowsMatches(
//            () -> executeOnSubscriber("INSERT INTO t1 (id) VALUES(3)"),
//            OperationOnInaccessibleRelationException.class,
//            "The relation \"doc.t1\" doesn't allow INSERT operations, because it is included in a logical replication."
//        );
//
//        executeOnSubscriber("DROP SUBSCRIPTION sub1");
//
//        response = executeOnSubscriber("INSERT INTO t1 (id) VALUES(3)");
//        assertThat(response.rowCount(), CoreMatchers.is(1L));
    }


    private boolean isTrackerActive() throws Exception {
        var replicationService = subscriberCluster.getInstance(LogicalReplicationService.class, subscriberCluster.getMasterName());
        Field m = replicationService.getClass().getDeclaredField("metadataTracker");
        m.setAccessible(true);
        MetadataTracker metadataTracker = (MetadataTracker) m.get(replicationService);
        Field f1 = metadataTracker.getClass().getDeclaredField("isActive");
        f1.setAccessible(true);
        return (boolean) f1.get(metadataTracker);
    }
}
