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

package io.crate.replication.logical.action;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.metadata.RelationName;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.replication.logical.metadata.Subscription;

public class UpdateSubscriptionActionTest {

    @Test
    public void test_existing_same_state_is_not_overridden() throws Exception {
        var oldSubscription = new Subscription(
            "user1",
            ConnectionInfo.fromURL("crate://localhost"),
            List.of("some_publication", "another_publication"),
            Settings.builder().put("enable", "true").build(),
            Map.of(
                RelationName.fromIndexName("doc.t1"),
                new Subscription.RelationState(Subscription.State.FAILED, "Subscription failed on restore")
            )
        );
        var newSubscription = new Subscription(
            oldSubscription.owner(),
            oldSubscription.connectionInfo(),
            oldSubscription.publications(),
            oldSubscription.settings(),
            Map.of(
                RelationName.fromIndexName("doc.t1"),
                new Subscription.RelationState(Subscription.State.FAILED, "Some other reason")
            )
        );

        assertThat(UpdateSubscriptionAction.updateSubscription(oldSubscription, newSubscription)).isEqualTo(oldSubscription);
    }
}
