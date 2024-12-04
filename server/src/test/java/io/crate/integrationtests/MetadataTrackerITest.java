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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;

import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.MetadataTracker;
import io.crate.testing.RetryRule;
import io.crate.testing.UseRandomizedSchema;

@UseRandomizedSchema(random = false)
public class MetadataTrackerITest extends LogicalReplicationITestCase {

    @Rule
    public RetryRule retryRule = new RetryRule(3);

    @Test
    public void test_schema_changes_of_subscribed_table_is_replicated() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" + defaultTableSettings() + ")");
        executeOnPublisher("INSERT INTO t1 (id) VALUES (1), (2)");
        createPublication("pub1", false, List.of("t1"));
        createSubscription("sub1", "pub1");

        // Ensure tracker has started
        assertBusy(() -> assertThat(isTrackerActive()).isTrue());

        executeOnPublisher("ALTER TABLE t1 ADD COLUMN value string");
        assertBusy(() -> {
            var r = executeOnSubscriber("SELECT column_name FROM information_schema.columns" +
                                        " WHERE table_name = 't1'" +
                                        " ORDER BY ordinal_position");
            assertThat(r).hasRows(
                "id",
                "value"
            );
        });
    }

    @Test
    public void test_schema_changes_of_subscribed_table_is_replicated_and_new_data_is_synced() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" + defaultTableSettings() + ")");
        executeOnPublisher("INSERT INTO t1 (id) VALUES (1), (2)");
        createPublication("pub1", false, List.of("t1"));
        createSubscription("sub1", "pub1");

        // Ensure tracker has started
        assertBusy(() -> assertThat(isTrackerActive()).isTrue());

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
            assertThat(r).hasRows(
                "1| NULL| NULL",
                "2| NULL| NULL",
                "3| chewbacca| NULL",
                "4| r2d2| NULL",
                "5| luke| 37",
                "6| yoda| 900"
            );
        }, 60, TimeUnit.SECONDS);
    }

    @Test
    public void test_schema_changes_of_multiple_tables_are_replicated() throws Exception {
        int numberOfTables = randomIntBetween(1,10);
        var tableNames = new ArrayList<String>();
        for (int i = 1; i <= numberOfTables; i++) {
            executeOnPublisher("CREATE TABLE t" + i + " (id INT) WITH(" + defaultTableSettings() + ")");
            tableNames.add("t" + i);
        }

        createPublication("pub1", false, tableNames);
        createSubscription("sub1", "pub1");

        // Ensure tracker has started
        assertBusy(() -> assertThat(isTrackerActive()).isTrue());

        for (String tableName : tableNames) {
            executeOnPublisher("ALTER TABLE " + tableName + " ADD COLUMN name varchar");
        }

        assertBusy(() -> {
            for (String tableName : tableNames) {
                var r = executeOnSubscriber("SELECT column_name FROM information_schema.columns" +
                                            " WHERE table_name = '" + tableName + "'" +
                                            " ORDER BY ordinal_position");
                assertThat(r).hasRows(
                    "id",
                    "name"
                );
            }
        }, 60, TimeUnit.SECONDS);
    }

    @Test
    public void test_new_table_and_new_partition_is_replicated() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT)");
        executeOnPublisher("CREATE TABLE t2 (id INT, p INT) PARTITIONED BY (p)");
        createPublication("pub1", true, List.of("t1", "t2"));
        createSubscription("sub1", "pub1");

        // Ensure tracker has started
        assertBusy(() -> assertThat(isTrackerActive()).isTrue());

        // Create new partition
        executeOnPublisher("INSERT INTO t2 (id, p) VALUES (1, 1)");
        // Create new table
        executeOnPublisher("CREATE TABLE t3 (id INT)");
        executeOnPublisher("GRANT DQL ON TABLE t3 TO " + SUBSCRIBING_USER);

        assertBusy(() -> {
            var r = executeOnSubscriber("SELECT column_name FROM information_schema.columns" +
                                        " WHERE table_name = 't3'" +
                                        " ORDER BY ordinal_position");
            assertThat(r).hasRows("id");
            r = executeOnSubscriber("SELECT values FROM information_schema.table_partitions" +
                                        " WHERE table_name = 't2'" +
                                        " ORDER BY partition_ident");
            assertThat(r).hasRows("{p=1}");
            ensureGreenOnSubscriber();
        }, 50, TimeUnit.SECONDS);
    }

    @Test
    public void test_new_empty_partitioned_table_is_replicated() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT)");
        createPublication("pub1", true, List.of("t1"));
        createSubscription("sub1", "pub1");

        // Ensure tracker has started
        assertBusy(() -> assertThat(isTrackerActive()).isTrue());

        // Create new table
        executeOnPublisher("CREATE TABLE t2 (id INT, p INT) PARTITIONED BY (p)");
        executeOnPublisher("GRANT DQL ON TABLE t2 TO " + SUBSCRIBING_USER);

        assertBusy(() -> {
            var r = executeOnSubscriber("SELECT column_name FROM information_schema.columns" +
                                        " WHERE table_name = 't2'" +
                                        " ORDER BY ordinal_position");
            assertThat(r).hasRows(
                "id",
                "p"
            );
        });
    }

    @Test
    public void test_deleted_partition_is_replicated() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT, p INT) PARTITIONED BY (p)");
        executeOnPublisher("INSERT INTO t1 (id, p) VALUES (1, 1), (2, 2)");
        createPublication("pub1", true, List.of("t1"));
        createSubscription("sub1", "pub1");

        // Ensure tracker has started
        assertBusy(() -> assertThat(isTrackerActive()).isTrue());

        // Wait until table is replicated
        assertBusy(() -> {
            executeOnSubscriber("REFRESH TABLE t1");
            var r = executeOnSubscriber("SELECT id, p FROM t1 ORDER BY id");
            assertThat(r).hasRows(
                "1| 1",
                "2| 2"
            );
        });

        // Drop partition
        executeOnPublisher("DELETE FROM t1 WHERE p = 1");

        assertBusy(() -> {
            executeOnSubscriber("REFRESH TABLE t1");
            var r = executeOnSubscriber("SELECT id, p FROM t1 ORDER BY id");
            assertThat(r).hasRows("2| 2");
        });
    }

    @Test
    public void test_deleted_and_recreated_partition_is_also_deleted_and_restored_on_subscriber() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT, p INT) PARTITIONED BY (p)");
        executeOnPublisher("INSERT INTO t1 (id, p) VALUES (1, 1), (2, 2)");
        createPublication("pub1", true, List.of("t1"));
        createSubscription("sub1", "pub1");

        // Ensure tracker has started
        assertBusy(() -> assertThat(isTrackerActive()).isTrue());

        // Wait until table is replicated
        assertBusy(() -> {
            executeOnSubscriber("REFRESH TABLE t1");
            var r = executeOnSubscriber("SELECT id, p FROM t1 ORDER BY id");
            assertThat(r).hasRows(
                "1| 1",
                "2| 2"
            );
        });

        // Drop partition
        executeOnPublisher("DELETE FROM t1 WHERE p = 1");
        // Re-create same partition with different value
        executeOnPublisher("INSERT INTO t1 (id, p) VALUES (11, 1)");

        assertBusy(() -> {
            executeOnSubscriber("REFRESH TABLE t1");
            var r = executeOnSubscriber("SELECT id, p FROM t1 ORDER BY id");
            assertThat(r).hasRows(
                "2| 2",
                "11| 1" // <- this must contain the id of the re-created partition
            );
        }, 50, TimeUnit.SECONDS);
    }

    @Test
    public void test_subscription_to_multiple_publications_should_not_stop_on_a_single_publication_drop() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT)");
        executeOnPublisher("INSERT INTO t1 (id) VALUES (1), (2)");
        createPublication("pub1", false, List.of("t1"));

        executeOnPublisher("CREATE TABLE t2 (id INT)");
        executeOnPublisher("INSERT INTO t2 (id) VALUES (1), (2)");
        createPublicationWithoutUser("pub2", false, List.of("t2"));

        executeOnPublisher("CREATE TABLE t3 (id INT, p INT) PARTITIONED BY (p)");
        executeOnPublisher("INSERT INTO t3 (id, p) VALUES (1, 1), (2, 2)");
        createPublicationWithoutUser("pub3", false, List.of("t3"));

        createSubscription("sub1", List.of("pub1", "pub2", "pub3"));

        assertBusy(() -> assertThat(isTrackerActive()).isTrue());


        executeOnPublisher("DROP PUBLICATION pub2"); // exclude publication with regular table
        executeOnPublisher("DROP PUBLICATION pub3"); // exclude publication with partitioned table

        // Dropping of the one one multiple publications doesn't stop tracking of the whole subscription.
        assertBusy(() -> assertThat(isTrackerActive()).isTrue());

        // Subscriber keeps receiving updates from non-dropped publication's regular table.
        assertBusy(() -> {
                executeOnPublisher("INSERT INTO t1 (id) VALUES (3)");
                var res = executeOnSubscriber("SELECT id FROM t1 ORDER BY id");
                assertThat(res.rowCount()).isGreaterThan(2L);
            }
        );

        // Tables belonging to the dropped publications turned into regular writable tables.
        assertBusy(
            () -> {
                long rowCount;
                try {
                    var response = executeOnSubscriber("INSERT INTO t2 (id) VALUES(4)");
                    rowCount = response.rowCount();
                } catch (OperationOnInaccessibleRelationException e) {
                    throw new AssertionError(e.getMessage());
                }
                assertThat(rowCount).isEqualTo(1L);
            }
        );
        assertBusy(
            () -> {
                long rowCount;
                try {
                    var response = executeOnSubscriber("INSERT INTO t3 (id, p) VALUES(4, 4)");
                    rowCount = response.rowCount();
                } catch (OperationOnInaccessibleRelationException e) {
                    throw new AssertionError(e.getMessage());
                }
                assertThat(rowCount).isEqualTo(1L);
            }
        );
    }

    public void test_subscription_to_multiple_publications_stops_when_all_publications_are_dropped() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT)");
        createPublication("pub1", false, List.of("t1"));

        executeOnPublisher("CREATE TABLE t2 (id INT)");
        createPublicationWithoutUser("pub2", false, List.of("t2"));


        createSubscription("sub1", List.of("pub1", "pub2"));

        assertBusy(() -> assertThat(isTrackerActive()).isTrue());

        executeOnPublisher("DROP PUBLICATION pub1");
        executeOnPublisher("DROP PUBLICATION pub2");

        // Dropping all publications of the subscription stops tracking.
        assertBusy(() -> assertThat(isTrackerActive()).isFalse());
    }

    @Test
    public void test_subscribing_to_the_own_tables_on_the_same_cluster() throws Exception {
        createPublication("pub", true, List.of());

        // subscription is created on the same cluster
        executeOnPublisher("CREATE SUBSCRIPTION sub " +
                           " CONNECTION '" + publisherConnectionUrl() + "' publication pub");

        executeOnPublisher("CREATE TABLE doc.t1 (id INT) WITH(" +
                           defaultTableSettings() +
                           ")");
        executeOnPublisher("CREATE TABLE doc.t2 (id INT, p INT) PARTITIONED BY (p)");

        executeOnPublisher("GRANT DQL ON TABLE doc.t1, doc.t2 TO " + SUBSCRIBING_USER);

        assertBusy(
            () -> {
                var res = executeOnPublisher(
                    "SELECT s.subname, sr.srrelid::text, sr.srsubstate, sr.srsubstate_reason" +
                        " FROM pg_subscription s" +
                        " LEFT JOIN pg_subscription_rel sr ON s.oid = sr.srsubid" +
                        " ORDER BY s.subname");
                assertThat(res).hasRowsInAnyOrder(
                    new Object[] {"sub", "doc.t1", "e", "Relation already exists"},
                    new Object[] {"sub", "doc.t2", "e", "Relation already exists"}
                );
                assertThat(isTrackerActive()).isFalse();
            }, 20, TimeUnit.SECONDS
        );

    }

    @Test
    public void test_alter_publication_drop_table_disables_table_replication() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" + defaultTableSettings() + ")");
        executeOnPublisher("INSERT INTO t1 (id) VALUES (1), (2)");
        executeOnPublisher("CREATE TABLE p2 (id INT, p INT) PARTITIONED BY (p)");
        executeOnPublisher("INSERT INTO p2 (id, p) VALUES (1, 1), (2, 2)");
        createPublication("pub1", false, List.of("t1", "p2"));
        createSubscription("sub1", "pub1");

        // Wait until tables are restored and tracker is active
        assertBusy(() -> assertThat(isTrackerActive()).isTrue());

        executeOnPublisher("ALTER PUBLICATION pub1 DROP TABLE t1, p2");

        // Only a next metadata poll will detect a dropped table and turn the table into a normal read-write one.
        assertBusy(
            () -> {
                long rowCount;
                try {
                    var response = executeOnSubscriber("INSERT INTO t1 (id) VALUES(4)");
                    rowCount = response.rowCount();
                } catch (Throwable e) {
                    throw new AssertionError(e.getMessage());
                }
                assertThat(rowCount).isEqualTo(1L);
            }
        );
        assertBusy(
            () -> {
                long rowCount;
                try {
                    var response = executeOnSubscriber("INSERT INTO p2 (id, p) VALUES(4, 4)");
                    rowCount = response.rowCount();
                } catch (Throwable e) {
                    throw new AssertionError(e.getMessage());
                }
                assertThat(rowCount).isEqualTo(1L);
            }
        );
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
