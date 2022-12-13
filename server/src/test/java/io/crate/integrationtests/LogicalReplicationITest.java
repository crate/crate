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

import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.TestingHelpers.printedTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.RelationAlreadyExists;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.exceptions.PublicationUnknownException;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import io.crate.testing.UseRandomizedSchema;
import io.crate.user.User;
import io.crate.user.UserLookup;


public class LogicalReplicationITest extends LogicalReplicationITestCase {

    @Test
    public void test_create_publication_checks_owner_was_not_deleted_before_creation() {
        executeOnPublisher("CREATE TABLE t1 (id INT)");

        String publicationOwner = "publication_owner";
        executeOnPublisher("CREATE USER " + publicationOwner);
        executeOnPublisher("GRANT AL TO " + publicationOwner);
        executeOnPublisher("GRANT DQL, DML, DDL ON TABLE t1 TO " + publicationOwner);
        UserLookup userLookup = publisherCluster.getInstance(UserLookup.class);
        User user = Objects.requireNonNull(userLookup.findUser(publicationOwner), "User " + publicationOwner + " must exist");

        executeOnPublisher("DROP USER " + publicationOwner);
        assertThrowsMatches(
            () -> executeOnPublisherAsUser("CREATE PUBLICATION pub1 FOR TABLE t1", user),
            IllegalStateException.class,
            "Publication 'pub1' cannot be created as the user 'publication_owner' owning the publication has been dropped."
        );
    }


    @Test
    public void test_create_subscription_checks_owner_was_not_deleted_before_creation() {
        executeOnPublisher("CREATE TABLE t1 (id INT)");
        createPublication("pub1", false, List.of("t1"));

        String subscriptionOwner = "subscription_owner";
        executeOnSubscriber("CREATE USER " + subscriptionOwner);
        executeOnSubscriber("GRANT AL TO " + subscriptionOwner);

        UserLookup userLookup = subscriberCluster.getInstance(UserLookup.class);
        User user = Objects.requireNonNull(userLookup.findUser(subscriptionOwner), "User " + subscriptionOwner + " must exist");

        executeOnSubscriber("DROP USER " + subscriptionOwner);
        assertThrowsMatches(
            () -> createSubscriptionAsUser("sub1", "pub1", user),
            IllegalStateException.class,
            "Subscription 'sub1' cannot be created as the user 'subscription_owner' owning the subscription has been dropped."
        );
    }

    @Test
    public void test_cannot_drop_user_who_owns_publications() {
        executeOnPublisher("CREATE TABLE t1 (id INT)");

        String publicationOwner = "publication_owner";
        executeOnPublisher("CREATE USER " + publicationOwner);
        executeOnPublisher("GRANT AL TO " + publicationOwner);
        executeOnPublisher("GRANT DQL, DML, DDL ON TABLE t1 TO " + publicationOwner);

        UserLookup userLookup = publisherCluster.getInstance(UserLookup.class);
        User user = Objects.requireNonNull(userLookup.findUser(publicationOwner), "User " + publicationOwner + " must exist");
        executeOnPublisherAsUser("CREATE PUBLICATION pub1 FOR TABLE t1", user);

        assertThrowsMatches(
            () -> executeOnPublisher("DROP USER " + publicationOwner),
            IllegalStateException.class,
            "User 'publication_owner' cannot be dropped. Publication 'pub1' needs to be dropped first."
        );
    }

    @Test
    public void test_cannot_drop_user_who_owns_subscriptions() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT)");
        createPublication("pub1", false, List.of("t1"));

        String subscriptionOwner = "subscription_owner";
        executeOnSubscriber("CREATE USER " + subscriptionOwner);
        executeOnSubscriber("GRANT AL TO " + subscriptionOwner);

        UserLookup userLookup = subscriberCluster.getInstance(UserLookup.class);
        User user = Objects.requireNonNull(userLookup.findUser(subscriptionOwner), "User " + subscriptionOwner + " must exist");
        createSubscriptionAsUser("sub1", "pub1", user);

        assertThrowsMatches(
            () -> executeOnSubscriber("DROP USER " + subscriptionOwner),
            IllegalStateException.class,
            "User 'subscription_owner' cannot be dropped. Subscription 'sub1' needs to be dropped first."
        );
    }

    @Test
    public void test_create_publication_for_concrete_table() {
        executeOnPublisher("CREATE TABLE t1 (id INT)");
        executeOnPublisher("CREATE PUBLICATION pub1 FOR TABLE t1");
        var response = executeOnPublisher(
            "SELECT oid, p.pubname, pubowner, puballtables, schemaname, tablename, pubinsert, pubupdate, pubdelete" +
            " FROM pg_publication p" +
            " JOIN pg_publication_tables t ON p.pubname = t.pubname" +
            " ORDER BY p.pubname, schemaname, tablename");
        assertThat(printedTable(response.rows()),
                   is("-119974068| pub1| -450373579| false| doc| t1| true| true| true\n"));
    }

    @Test
    public void test_create_publication_for_all_tables() {
        executeOnPublisher("CREATE TABLE t1 (id INT)");
        executeOnPublisher("CREATE PUBLICATION pub1 FOR ALL TABLES");
        executeOnPublisher("CREATE TABLE my_schema.t2 (id INT)");
        var response = executeOnPublisher(
            "SELECT oid, p.pubname, pubowner, puballtables, schemaname, tablename, pubinsert, pubupdate, pubdelete" +
            " FROM pg_publication p" +
            " JOIN pg_publication_tables t ON p.pubname = t.pubname" +
            " ORDER BY p.pubname, schemaname, tablename");
        assertThat(printedTable(response.rows()),
                   is("284890074| pub1| -450373579| true| doc| t1| true| true| true\n" +
                      "284890074| pub1| -450373579| true| my_schema| t2| true| true| true\n"));
    }

    @Test
    public void test_drop_publication() {
        executeOnPublisher("CREATE TABLE t3 (id INT)");
        executeOnPublisher("CREATE PUBLICATION pub2 FOR TABLE t3");
        executeOnPublisher("DROP PUBLICATION pub2");

        var response = executeOnPublisher("SELECT * FROM pg_publication WHERE pubname = 'pub2'");
        assertThat(response.rowCount(), is(0L));
        response = executeOnPublisher("SELECT * FROM pg_publication_tables WHERE pubname = 'pub2'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void test_drop_subscription() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" + defaultTableSettings() + ")");

        createPublication("pub1", false, List.of("t1"));
        createSubscription("sub1", "pub1");

        LogicalReplicationService replicationService = subscriberCluster.getInstance(LogicalReplicationService.class);
        assertTrue(replicationService.subscriptions().containsKey("sub1"));

        executeOnSubscriber("DROP SUBSCRIPTION sub1 ");
        assertFalse(replicationService.subscriptions().containsKey("sub1"));

        var response = executeOnSubscriber("SELECT * FROM pg_subscription");
        assertThat(response.rowCount(), is(0L));
        response = executeOnSubscriber("SELECT * FROM pg_subscription_rel");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void test_create_subscription_subscribing_user_not_found() {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" + defaultTableSettings() + ")");

        // createPublication is not used in order not to create SUBSCRIBING_USER and verify that check is done in PublicationsStateAction
        executeOnPublisher("CREATE PUBLICATION pub1 FOR TABLE t1");

        assertThatThrownBy(() -> createSubscription("sub1", "pub1"))
            .satisfiesAnyOf(
                // If executing via sniff mode, the user is not found
                e -> assertThat(e)
                    .isExactlyInstanceOf(IllegalStateException.class)
                    .hasMessage("Cannot build publication state, subscribing user '" + SUBSCRIBING_USER + "' was not found."),
                // If executing via pg-tunneling the authentication will fail because of the missing user
                e -> assertThat(e)
                    .isExactlyInstanceOf(IllegalStateException.class)
                    .hasMessageStartingWith("Error response: FATAL, trust authentication failed for user \"subscriber\"")
            );
    }

    @Test
    public void test_subscribing_to_single_publication() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" +
                           defaultTableSettings() +
                           ")");
        executeOnPublisher("INSERT INTO t1 (id) VALUES (1), (2)");

        createPublication("pub1", false, List.of("t1"));

        createSubscription("sub1", "pub1");

        executeOnSubscriber("REFRESH TABLE t1");
        var response = executeOnSubscriber("SELECT * FROM t1 ORDER BY id");
        assertThat(printedTable(response.rows()), is("1\n" +
                                                     "2\n"));
    }

    @Test
    public void test_subscribing_to_unknown_publication_raises_error() throws Exception {
        createPublication("pub1", true, List.of());
        assertThrowsMatches(
            () -> executeOnSubscriber("CREATE SUBSCRIPTION sub1" +
                " CONNECTION '" + publisherConnectionUrl() + "' publication unknown_pub"),
            PublicationUnknownException.class,
            "Publication 'unknown_pub' unknown"
        );
    }

    @Test
    public void test_create_subscription_system_tables_filled() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" + defaultTableSettings() + ")");
        executeOnPublisher("CREATE TABLE t2 (id INT) WITH(" + defaultTableSettings() + ")");

        // If isForAllTables is true, list argument is used only to grant DQL (to make subscribe work) but publication is not aware of tables in the moment of creation.
        createPublication("pub1", true, List.of("t1, t2"));
        createSubscription("sub1", "pub1");

        // s.subconninfo is not being selected since
        // it's different from run to run due to different port in host.
        var systemTableResponse = executeOnSubscriber(
            "SELECT " +
            " s.oid, s.subdbid, s.subname, s.subowner, s.subenabled, s.subbinary, s.substream, s.subslotname," +
            " s.subsynccommit, s.subpublications," +
            " sr.srsubid, sr.srrelid, r.relname" +
            " FROM pg_subscription s" +
            " JOIN pg_subscription_rel sr ON s.oid = sr.srsubid" +
            " JOIN pg_class r ON sr.srrelid = r.oid" +
            " ORDER BY s.subname, r.relname"
        );
        assertThat(printedTable(systemTableResponse.rows()),
            is("530917412| 0| sub1| crate| true| true| false| NULL| NULL| [pub1]| 530917412| 728874843| t1\n" +
                     "530917412| 0| sub1| crate| true| true| false| NULL| NULL| [pub1]| 530917412| 1737494392| t2\n"));
    }

    @Test
    public void test_subscribing_to_publication_while_table_exists_raises_error() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" +
                           defaultTableSettings() +
                           ")");
        executeOnPublisher("INSERT INTO t1 (id) VALUES (1), (2)");

        createPublication("pub1", false, List.of("t1"));

        executeOnSubscriber("CREATE TABLE t1 (id int)");
        assertThrows(
            "Subscription 'sub1' cannot be created as included relation 't1' already exists",
            RelationAlreadyExists.class,
            () -> createSubscription("sub1", "pub1")
        );
    }

    @Test
    public void test_subscribing_to_single_publication_with_multiple_tables() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" +
                           defaultTableSettings() +
                           ")");
        executeOnPublisher("INSERT INTO t1 (id) VALUES (1), (2)");

        executeOnPublisher("CREATE TABLE t2 (id INT) CLUSTERED INTO 1 SHARDS WITH(" +
                           defaultTableSettings() +
                           ")");
        executeOnPublisher("INSERT INTO t2 (id) VALUES (3), (4)");

        createPublication("pub1", false, List.of("t1", "t2"));

        createSubscription("sub1", "pub1");

        executeOnSubscriber("REFRESH TABLE t1");
        var response = executeOnSubscriber("SELECT * FROM t1 ORDER BY id");
        assertThat(printedTable(response.rows()), is("1\n" +
                                                     "2\n"));

        executeOnSubscriber("REFRESH TABLE t2");
        response = executeOnSubscriber("SELECT * FROM t2 ORDER BY id");
        assertThat(printedTable(response.rows()), is("3\n" +
                                                     "4\n"));
    }

    @Test
    public void test_subscribing_to_single_publication_with_partitioned_table() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT, p INT) PARTITIONED BY (p) WITH(" +
                           defaultTableSettings() +
                           ")");
        executeOnPublisher("INSERT INTO t1 (id, p) VALUES (1, 1), (2, 2)");

        createPublication("pub1", false, List.of("t1"));

        createSubscription("sub1", "pub1");

        executeOnSubscriber("REFRESH TABLE t1");
        var response = executeOnSubscriber("SELECT * FROM t1 ORDER BY id");
        assertThat(printedTable(response.rows()), is("1| 1\n" +
                                                     "2| 2\n"));
    }

    @Test
    public void test_subscribing_to_single_publication_for_all_tables() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT, p INT) PARTITIONED BY (p) WITH(" +
                           defaultTableSettings() +
                           ")");
        executeOnPublisher("INSERT INTO t1 (id, p) VALUES (1, 1), (2, 2)");
        executeOnPublisher("CREATE TABLE my_schema.t2 (id INT) WITH(" +
                           defaultTableSettings() +
                           ")");
        executeOnPublisher("INSERT INTO my_schema.t2 (id) VALUES (1), (2)");

        createPublication("pub1", false, List.of("t1", "my_schema.t2"));

        createSubscription("sub1", "pub1");

        executeOnSubscriber("REFRESH TABLE t1");
        var response = executeOnSubscriber("SELECT * FROM t1 ORDER BY id");
        assertThat(printedTable(response.rows()), is("1| 1\n" +
                                                     "2| 2\n"));

        executeOnSubscriber("REFRESH TABLE my_schema.t2");
        response = executeOnSubscriber("SELECT * FROM my_schema.t2 ORDER BY id");
        assertThat(printedTable(response.rows()), is("1\n" +
                                                     "2\n"));
    }

    @Test
    public void test_subscribed_tables_are_followed_and_updated() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" +
                           defaultTableSettings() +
                           ")");
        executeOnPublisher("INSERT INTO t1 (id) VALUES (1), (2)");


        createPublication("pub1", false, List.of("t1"));

        createSubscription("sub1", "pub1");

        executeOnSubscriber("REFRESH TABLE t1");
        var response = executeOnSubscriber("SELECT * FROM t1 ORDER BY id");
        assertThat(printedTable(response.rows()), is("1\n" +
                                                     "2\n"));

        int numDocs = 100; // <- should be greater than REPLICATION_CHANGE_BATCH_SIZE to test repeated polls
        var bulkArgs = new Object[numDocs][1];
        for (int i = 0; i < numDocs; i++) {
            bulkArgs[i][0] = i;
        }
        executeBulkOnPublisher("INSERT INTO t1 (id) VALUES (?)", bulkArgs);

        assertBusy(() -> {
            executeOnSubscriber("REFRESH TABLE t1");
            var res = executeOnSubscriber("SELECT * FROM t1");
            assertThat(res.rowCount(), is((long) (numDocs + 2)));
        }, 10, TimeUnit.SECONDS);
    }

    @Test
    public void test_write_to_subscribed_table_is_forbidden() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" +
                           defaultTableSettings() +
                           ")");
        executeOnPublisher("INSERT INTO t1 (id) VALUES (1), (2)");
        createPublication("pub1", false, List.of("t1"));
        createSubscription("sub1", "pub1");

        assertThrowsMatches(
            () -> executeOnSubscriber("INSERT INTO t1 (id) VALUES(3)"),
            OperationOnInaccessibleRelationException.class,
            "The relation \"" + publisherSqlExecutor.getCurrentSchema() +
            ".t1\" doesn't allow INSERT operations, because it is included in a logical replication subscription."
        );
    }

    @Test
    public void test_write_to_subscribed_empty_partitioned_table_is_forbidden() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT, p INT) PARTITIONED BY (p) WITH(" +
                           defaultTableSettings() +
                           ")");
        createPublication("pub1", false, List.of("t1"));
        executeOnSubscriber("CREATE SUBSCRIPTION sub1" +
            " CONNECTION '" + publisherConnectionUrl() + "' PUBLICATION pub1");
        // Wait until empty partitioned table (template only) is replicated
        assertBusy(() -> {
            var r = executeOnSubscriber("SELECT column_name FROM information_schema.columns" +
                " WHERE table_name = 't1'" +
                " ORDER BY ordinal_position");
            assertThat(printedTable(r.rows()), Matchers.is("id\n" +
                "p\n"));
        });

        assertThrowsMatches(
            () -> executeOnSubscriber("INSERT INTO t1 (id) VALUES(3)"),
            OperationOnInaccessibleRelationException.class,
            "The relation \"" + publisherSqlExecutor.getCurrentSchema() +
            ".t1\" doesn't allow INSERT operations, because it is included in a logical replication subscription."
        );
    }

    @Test
    public void test_subscription_state_order() throws Exception {
        String subscriptionName = "sub1";
        final ArrayList<Subscription.State> subscriptionStates = new ArrayList<>();
        ClusterService clusterService = subscriberCluster.getMasterNodeInstance(ClusterService.class);
        clusterService.addListener(
            event -> {
                if (event.metadataChanged() == false) {
                    return;
                }
                Metadata currentMetadata = event.state().metadata();
                var metadata = (SubscriptionsMetadata) currentMetadata.custom(SubscriptionsMetadata.TYPE);
                if (metadata != null) {
                    var subscription = metadata.subscription().get(subscriptionName);
                    if (subscription != null) {
                        var currentState = subscription.relations().get(RelationName.fromIndexName("t1")).state();
                        synchronized (subscriptionStates) {
                            var size = subscriptionStates.size();
                            if (size == 0 || subscriptionStates.get(size - 1).equals(currentState) == false) {
                                subscriptionStates.add(currentState);
                            }
                        }
                    }
                }
            });

        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" +
                           defaultTableSettings() +
                           ")");
        createPublication("pub1", false, List.of("t1"));
        createSubscription("sub1", "pub1");

        // Tracking (MONITORING state) may not have started even if table is green, so lets use a busy loop
        assertBusy(
            () -> {
                synchronized (subscriptionStates) {
                    assertThat(subscriptionStates).contains(
                        Subscription.State.INITIALIZING,
                        Subscription.State.RESTORING,
                        Subscription.State.MONITORING
                    );
                }
            },
            30,
            TimeUnit.SECONDS
        );

        // check final exposed `r`(MONITORING) state
        var res = executeOnSubscriber(
            "SELECT" +
            " s.subname, r.relname, sr.srsubstate, sr.srsubstate_reason" +
            " FROM pg_subscription s" +
            " JOIN pg_subscription_rel sr ON s.oid = sr.srsubid" +
            " JOIN pg_class r ON sr.srrelid = r.oid");
        assertThat(printedTable(res.rows()), is("sub1| t1| r| NULL\n"));
    }

    @Test
    public void test_write_to_subscribed_table_is_allowed_after_dropping_subscription() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" + defaultTableSettings() + ")");
        executeOnPublisher("CREATE TABLE t2 (id INT, p INT) PARTITIONED BY (p) WITH(" + defaultTableSettings() + ")");
        executeOnPublisher("INSERT INTO t1 (id) VALUES (1), (2)");
        executeOnPublisher("INSERT INTO t2 (id, p) VALUES (1, 1), (2, 2)");

        // It's important to subscribe to more than 1 table to check
        // that re-used close/open table logic works with multiple tables
        createPublication("pub1", false, List.of("t1", "t2"));
        createSubscription("sub1", "pub1");

        executeOnSubscriber("DROP SUBSCRIPTION sub1 ");

        var response = executeOnSubscriber("INSERT INTO t1 (id) VALUES(3)");
        assertThat(response.rowCount(), is(1L));

        response = executeOnSubscriber("INSERT INTO t2 (id, p) VALUES(3, 3)");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void test_alter_publication_add_table() {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" + defaultTableSettings() + ")");
        executeOnPublisher("CREATE TABLE t2 (id INT) WITH(" + defaultTableSettings() + ")");
        createPublication("pub1", false, List.of("t1"));
        executeOnPublisher("ALTER PUBLICATION pub1 ADD TABLE t2");
        var response = executeOnPublisher(
            "SELECT oid, p.pubname, pubowner, puballtables, schemaname, tablename, pubinsert, pubupdate, pubdelete" +
                " FROM pg_publication p" +
                " JOIN pg_publication_tables t ON p.pubname = t.pubname" +
                " ORDER BY p.pubname, schemaname, tablename");
        assertThat(printedTable(response.rows()),
            is("14768324| pub1| -450373579| false| doc| t1| true| true| true\n" +
                "14768324| pub1| -450373579| false| doc| t2| true| true| true\n"));
    }

    /**
     * Ensures a dropped subscription can be immediately re-created.
     * Specially, that all existing retention leases at the publisher shards are dropped when the subscription is dropped.
     */
    @Test
    public void test_recreate_dropped_subscription() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" + defaultTableSettings() + ")");
        executeOnPublisher("INSERT INTO t1 (id) VALUES (1), (2), (3), (4)");

        createPublication("pub1", false, List.of("t1"));
        createSubscription("sub1", "pub1");

        executeOnSubscriber("DROP SUBSCRIPTION sub1 ");
        executeOnSubscriber("DROP TABLE t1");

        createSubscription("sub1", "pub1");
        assertBusy(
            () -> {
                var res = executeOnSubscriber("SELECT id FROM t1 ORDER BY id");
                assertThat(res.rowCount(), is(4L));
            }
        );
    }

    @Test
    public void test_drop_subscribed_table_is_not_allowed() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" + defaultTableSettings() + ")");

        createPublication("pub1", false, List.of("t1"));
        createSubscription("sub1", "pub1");

        assertThrowsMatches(
            () -> executeOnSubscriber("DROP TABLE t1"),
            OperationOnInaccessibleRelationException.class,
            "The relation \"" + publisherSqlExecutor.getCurrentSchema() +
            ".t1\" doesn't allow DROP operations, because it is included in a logical replication subscription."
        );
    }

    /**
     * Test a regression which resulting in a failed initial restore of the re-created subscription caused
     * not correctly removed retention leases while subscription is dropped.
     */
    @Test
    public void test_drop_and_recreate_subscription_while_published_table_is_written_to() throws Exception {
        executeOnPublisher("CREATE TABLE t1 (id INT) WITH(" + defaultTableSettings() + ")");
        executeOnPublisher("INSERT INTO t1 (id) VALUES (1), (2), (3), (4)");
        createPublication("pub1", false, List.of("t1"));

        var cnt = new AtomicLong(4L);
        var stop = new AtomicBoolean(false);
        try {
            var writeThread = new Thread(() -> {
                while (stop.get() == false) {
                    publisherSqlExecutor.exec("INSERT INTO t1 (id) VALUES (?)", new Object[randomIntBetween(1, 9999)]);
                    cnt.incrementAndGet();
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        stop.set(true);
                    }
                }
            });
            writeThread.start();

            createSubscription("sub1", "pub1");

            assertBusy(() -> {
                var res = executeOnSubscriber(
                    "SELECT" +
                        " s.subname, r.relname, sr.srsubstate, sr.srsubstate_reason" +
                        " FROM pg_subscription s" +
                        " JOIN pg_subscription_rel sr ON s.oid = sr.srsubid" +
                        " JOIN pg_class r ON sr.srrelid = r.oid");
                assertThat(printedTable(res.rows()), Matchers.is("sub1| t1| r| NULL\n"));
            });

            executeOnSubscriber("DROP SUBSCRIPTION sub1");
            executeOnSubscriber("DROP TABLE t1");

            executeOnSubscriber("CREATE SUBSCRIPTION sub1" +
                " CONNECTION '" + publisherConnectionUrl() + "' publication pub1");

        } finally {
            stop.set(true);
        }

        assertBusy(() -> {
            var res = executeOnSubscriber(
                "SELECT" +
                    " s.subname, r.relname, sr.srsubstate, sr.srsubstate_reason" +
                    " FROM pg_subscription s" +
                    " JOIN pg_subscription_rel sr ON s.oid = sr.srsubid" +
                    " JOIN pg_class r ON sr.srrelid = r.oid");
            assertThat(printedTable(res.rows()), Matchers.is("sub1| t1| r| NULL\n"));
        });
    }
}
