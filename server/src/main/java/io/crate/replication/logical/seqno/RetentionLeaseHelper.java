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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.seqno.RetentionLeaseAlreadyExistsException;
import org.elasticsearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException;
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException;
import org.elasticsearch.index.shard.ShardId;

/*
 * Derived from org.opensearch.replication.seqno.RemoteClusterRetentionLeaseHelper
 */
public class RetentionLeaseHelper {

    private static final Logger LOGGER = Loggers.getLogger(RetentionLeaseHelper.class);

    private static String retentionLeaseSource(String subscriberClusterName) {
        return "replication:" + subscriberClusterName;
    }

    private static String retentionLeaseIdForShard(String subscriberClusterName, ShardId subscriberShardId) {
        var retentionLeaseSource = retentionLeaseSource(subscriberClusterName);
        return retentionLeaseSource + ":" + subscriberShardId;
    }


    private final String publisherClusterName;
    private final Client client;
    private final String retentionLeaseSource;

    public RetentionLeaseHelper(String publisherClusterName, Client client) {
        this.publisherClusterName = publisherClusterName;
        this.client = client;
        this.retentionLeaseSource = "logical_replication:" + publisherClusterName;
    }


    public void addRetentionLease(ShardId publisherShardId, long seqNo, ShardId subscriberShardId) {
        var retentionLeaseId = retentionLeaseIdForShard(publisherClusterName, subscriberShardId);
        var request = new RetentionLeaseActions.AddRequest(publisherShardId, retentionLeaseId, seqNo, retentionLeaseSource);
        try {
            client.execute(RetentionLeaseActions.Add.INSTANCE, request);
        } catch (RetentionLeaseAlreadyExistsException e) {
            LOGGER.error(e.getMessage());
            LOGGER.info("Renew retention lease as it already exists {} with {}", retentionLeaseId, seqNo);
            // Only one retention lease should exists for the subscriber shard
            // Ideally, this should have got cleaned-up
            renewRetentionLease(publisherShardId, seqNo, subscriberShardId);
        }
    }

    public boolean verifyRetentionLeaseExist(ShardId leaderShardId, ShardId followerShardId)  {
        var retentionLeaseId = retentionLeaseIdForShard(publisherClusterName, followerShardId);
        // Currently there is no API to describe/list the retention leases .
        // So we are verifying the existence of lease by trying to renew a lease by same name .
        // If retention lease doesn't exist, this will throw an RetentionLeaseNotFoundException exception
        // If it does it will try to RENEW that one with -1 seqno , which should  either
        // throw RetentionLeaseInvalidRetainingSeqNoException if a retention lease exists with higher seq no.
        // which will exist in all probability
        // Or if a retention lease already exists with -1 seqno, it will renew that .
        var request = new RetentionLeaseActions.RenewRequest(
            leaderShardId, retentionLeaseId, RetentionLeaseActions.RETAIN_ALL, retentionLeaseSource);
        try {
            client.execute(RetentionLeaseActions.Renew.INSTANCE, request);
        } catch (RetentionLeaseInvalidRetainingSeqNoException e) {
            return true;
        }
        catch (RetentionLeaseNotFoundException e) {
            return false;
        }
        return true;
    }

    public void attemptRetentionLeaseRemoval(ShardId publisherShardId, ShardId subscriberShardId) {
        var retentionLeaseId = retentionLeaseIdForShard(publisherClusterName, subscriberShardId);
        var request = new RetentionLeaseActions.RemoveRequest(publisherShardId, retentionLeaseId);
        try {
            client.execute(RetentionLeaseActions.Remove.INSTANCE, request);
            LOGGER.info("Removed retention lease with id - {}", retentionLeaseId);
        } catch(RetentionLeaseNotFoundException e) {
            // log error and bail
            LOGGER.error(e.getMessage());
        } catch (Exception e) {
            // We are not bubbling up the exception as the stop action/ task cleanup should succeed
            // even if we fail to remove the retention lease from leader cluster
            LOGGER.error("Exception in removing retention lease", e);
        }
    }

    /**
     * Remove these once the callers are moved to above APIs
     */
    public void addRetentionLease(ShardId publisherShardId, long seqNo, ShardId subscriberShardId, long timeout) {
        var retentionLeaseId = retentionLeaseIdForShard(publisherClusterName, subscriberShardId);
        var request = new RetentionLeaseActions.AddRequest(publisherShardId, retentionLeaseId, seqNo, retentionLeaseSource);
        try {
            client.execute(RetentionLeaseActions.Add.INSTANCE, request).actionGet(timeout);
        } catch (RetentionLeaseAlreadyExistsException e) {
            LOGGER.error(e.getMessage());
            LOGGER.info("Renew retention lease as it already exists {} with {}", retentionLeaseId, seqNo);
            // Only one retention lease should exists for the follower shard
            // Ideally, this should have got cleaned-up
            renewRetentionLease(publisherShardId, seqNo, subscriberShardId, timeout);
        }
    }

    public void renewRetentionLease(ShardId publisherShardId, long seqNo, ShardId subscriberShardId) {
        var retentionLeaseId = retentionLeaseIdForShard(publisherClusterName, subscriberShardId);
        var request = new RetentionLeaseActions.RenewRequest(publisherShardId, retentionLeaseId, seqNo, retentionLeaseSource);
        client.execute(RetentionLeaseActions.Renew.INSTANCE, request);

    }

    public void renewRetentionLease(ShardId publisherShardId, long seqNo, ShardId subscriberShardId, long timeout) {
        var retentionLeaseId = retentionLeaseIdForShard(publisherClusterName, subscriberShardId);
        var request = new RetentionLeaseActions.RenewRequest(publisherShardId, retentionLeaseId, seqNo, retentionLeaseSource);
        client.execute(RetentionLeaseActions.Renew.INSTANCE, request).actionGet(timeout);
    }
}
