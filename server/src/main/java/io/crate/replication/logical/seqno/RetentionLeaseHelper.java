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

package io.crate.replication.logical.seqno;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.seqno.RetentionLeaseAlreadyExistsException;
import org.elasticsearch.index.shard.ShardId;

import io.crate.common.exceptions.Exceptions;
import io.crate.exceptions.SQLExceptions;

/*
 * Derived from org.opensearch.replication.seqno.RemoteClusterRetentionLeaseHelper
 */
public class RetentionLeaseHelper {

    private static final Logger LOGGER = LogManager.getLogger(RetentionLeaseHelper.class);

    private static String retentionLeaseSource(String subscriberClusterName) {
        return "logical_replication:" + subscriberClusterName;
    }

    private static String retentionLeaseIdForShard(String subscriberClusterName, ShardId shardId) {
        var retentionLeaseSource = retentionLeaseSource(subscriberClusterName);
        return retentionLeaseSource + ":" + shardId;
    }


    public static void addRetentionLease(ShardId shardId,
                                         long seqNo,
                                         String subscriberClusterName,
                                         Client client,
                                         ActionListener<RetentionLeaseActions.Response> listener) {
        var retentionLeaseId = retentionLeaseIdForShard(subscriberClusterName, shardId);
        var request = new RetentionLeaseActions.AddOrRenewRequest(
            shardId,
            retentionLeaseId,
            seqNo,
            retentionLeaseSource(subscriberClusterName)
        );
        client.execute(RetentionLeaseActions.Add.INSTANCE, request)
            .whenComplete((response, err) -> {
                if (err == null) {
                    listener.onResponse(response);
                } else {
                    var t = SQLExceptions.unwrap(err);
                    if (t instanceof RetentionLeaseAlreadyExistsException) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(
                                "Renew retention lease as it already exists {} with {}",
                                retentionLeaseId,
                                seqNo
                            );
                        }
                        // Only one retention lease should exists for the follower shard
                        // Ideally, this should have got cleaned-up
                        renewRetentionLease(shardId, seqNo, subscriberClusterName, client, listener);
                    } else {
                        listener.onFailure(Exceptions.toException(t));
                    }
                }
            });
    }

    public static void renewRetentionLease(ShardId shardId,
                                           long seqNo,
                                           String subscriberClusterName,
                                           Client client,
                                           ActionListener<RetentionLeaseActions.Response> listener) {
        var retentionLeaseId = retentionLeaseIdForShard(subscriberClusterName, shardId);
        var request = new RetentionLeaseActions.AddOrRenewRequest(
            shardId,
            retentionLeaseId,
            seqNo,
            retentionLeaseSource(subscriberClusterName)
        );
        client.execute(RetentionLeaseActions.Renew.INSTANCE, request).whenComplete(listener);
    }

    public static void attemptRetentionLeaseRemoval(ShardId shardId,
                                                    String subscriberClusterName,
                                                    Client client,
                                                    ActionListener<RetentionLeaseActions.Response> listener) {
        var retentionLeaseId = retentionLeaseIdForShard(subscriberClusterName, shardId);
        var request = new RetentionLeaseActions.RemoveRequest(shardId, retentionLeaseId);
        client.execute(RetentionLeaseActions.Remove.INSTANCE, request)
            .whenComplete((response, err) -> {
                if (err == null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Removed retention lease with id - {}", retentionLeaseId);
                    }
                    listener.onResponse(response);
                } else {
                    var e = Exceptions.toException(SQLExceptions.unwrap(err));
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Exception in removing retention lease", e);
                    }
                    listener.onFailure(e);
                }
            });
    }
}
