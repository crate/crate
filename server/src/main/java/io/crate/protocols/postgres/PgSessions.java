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

package io.crate.protocols.postgres;

import io.crate.action.sql.Session;
import io.crate.execution.jobs.kill.KillJobsRequest;
import io.crate.execution.jobs.kill.TransportKillJobsNodeAction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PgSessions {
    private static final Logger LOGGER = LogManager.getLogger(PgSessions.class);

    private final ConcurrentMap<KeyData, Session> activeSessions;
    private final TransportKillJobsNodeAction transportKillJobsNodeAction;

    public PgSessions(TransportKillJobsNodeAction transportKillJobsNodeAction) {
        this.activeSessions = new ConcurrentHashMap<>();
        this.transportKillJobsNodeAction = transportKillJobsNodeAction;
    }

    public void remove(KeyData keyData) {
        activeSessions.remove(keyData);
    }

    public void add(KeyData keyData, Session session) {
        assert !activeSessions.containsKey(keyData) : "The given KeyData already has an associated active Session";
        activeSessions.put(keyData, session);
    }

    public void cancel(KeyData targetKeyData) {
        Session targetSession = activeSessions.get(targetKeyData); // the session executing the target query
        if (targetSession != null) {
            String userName = targetSession.sessionContext().sessionUser().name();
            UUID targetJobID = targetSession.getMostRecentJobID();
            if (targetJobID != null) {
                transportKillJobsNodeAction.broadcast(
                    new KillJobsRequest(List.of(targetJobID), userName, "Cancellation requested by: " + userName));
                targetSession.resetDeferredExecutions();
            } else {
                LOGGER.debug("Cancellation request is ignored since the session does not have an active job to kill");
            }
        } else {
            LOGGER.debug("Cancellation request is ignored since the corresponding session cannot be found with the given KeyData");
        }
    }
}
