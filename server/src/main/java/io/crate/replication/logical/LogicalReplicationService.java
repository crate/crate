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

package io.crate.replication.logical;

import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;

import java.util.Collections;
import java.util.Map;

public class LogicalReplicationService implements ClusterStateListener {

    private SubscriptionsMetadata subscriptionsMetadata;
    private PublicationsMetadata publicationsMetadata;

    public LogicalReplicationService(ClusterService clusterService) {
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.metadataChanged()) {
            return;
        }

        var prevMetadata = event.previousState().metadata();
        var newMetadata = event.state().metadata();

        SubscriptionsMetadata prevSubscriptionsMetadata = prevMetadata.custom(SubscriptionsMetadata.TYPE);
        SubscriptionsMetadata newSubscriptionsMetadata = newMetadata.custom(SubscriptionsMetadata.TYPE);

        if (prevSubscriptionsMetadata != newSubscriptionsMetadata) {
            subscriptionsMetadata = newSubscriptionsMetadata;
        }

        PublicationsMetadata prevPublicationsMetadata = prevMetadata.custom(PublicationsMetadata.TYPE);
        PublicationsMetadata newPublicationsMetadata = newMetadata.custom(PublicationsMetadata.TYPE);

        if (prevPublicationsMetadata != newPublicationsMetadata) {
            publicationsMetadata = newPublicationsMetadata;
        }
    }


    public Map<String, Subscription> subscriptions() {
        if (subscriptionsMetadata == null) {
            return Collections.emptyMap();
        }
        return subscriptionsMetadata.subscription();
    }

    public Map<String, Publication> publications() {
        if (publicationsMetadata == null) {
            return Collections.emptyMap();
        }
        return publicationsMetadata.publications();
    }
}
