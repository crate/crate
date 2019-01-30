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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

/**
 * Task that tracks replication actions.
 */
public class ReplicationTask extends Task {
    private volatile String phase = "starting";

    public ReplicationTask(long id, String description, TaskId parentTaskId) {
        super(id, description, parentTaskId);
    }

    /**
     * Set the current phase of the task.
     */
    public void setPhase(String phase) {
        this.phase = phase;
    }

    /**
     * Get the current phase of the task.
     */
    public String getPhase() {
        return phase;
    }
}
