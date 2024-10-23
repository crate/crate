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

package org.elasticsearch.action.admin.cluster.tasks;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.elasticsearch.cluster.service.PendingClusterTask;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

public class PendingClusterTasksResponse extends TransportResponse implements Iterable<PendingClusterTask> {

    private final List<PendingClusterTask> pendingTasks;

    PendingClusterTasksResponse(List<PendingClusterTask> pendingTasks) {
        this.pendingTasks = pendingTasks;
    }

    public List<PendingClusterTask> pendingTasks() {
        return pendingTasks;
    }

    @Override
    public Iterator<PendingClusterTask> iterator() {
        return pendingTasks.iterator();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("tasks: (").append(pendingTasks.size()).append("):\n");
        for (PendingClusterTask pendingClusterTask : this) {
            sb.append(pendingClusterTask.getInsertOrder())
                .append("/")
                .append(pendingClusterTask.getPriority())
                .append("/")
                .append(pendingClusterTask.getSource())
                .append("/")
                .append(pendingClusterTask.getTimeInQueue())
                .append("\n");
        }
        return sb.toString();
    }

    public PendingClusterTasksResponse(StreamInput in) throws IOException {
        pendingTasks = in.readList(PendingClusterTask::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(pendingTasks);
    }

}
