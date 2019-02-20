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

package org.elasticsearch.cluster.routing.allocation.command;

import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A simple {@link AllocationCommand} composite managing several
 * {@link AllocationCommand} implementations
 */
public class AllocationCommands {
    private final List<AllocationCommand> commands = new ArrayList<>();

    /**
     * Creates a new set of {@link AllocationCommands}
     *
     * @param commands {@link AllocationCommand}s that are wrapped by this instance
     */
    public AllocationCommands(AllocationCommand... commands) {
        if (commands != null) {
            this.commands.addAll(Arrays.asList(commands));
        }
    }

    /**
     * Adds a set of commands to this collection
     * @param commands Array of commands to add to this instance
     * @return {@link AllocationCommands} with the given commands added
     */
    public AllocationCommands add(AllocationCommand... commands) {
        if (commands != null) {
            this.commands.addAll(Arrays.asList(commands));
        }
        return this;
    }

    /**
     * Executes all wrapped commands on a given {@link RoutingAllocation}
     * @param allocation {@link RoutingAllocation} to apply this command to
     * @throws org.elasticsearch.ElasticsearchException if something happens during execution
     */
    public RoutingExplanations execute(RoutingAllocation allocation, boolean explain) {
        RoutingExplanations explanations = new RoutingExplanations();
        for (AllocationCommand command : commands) {
            explanations.add(command.execute(allocation, explain));
        }
        return explanations;
    }

    /**
     * Reads a {@link AllocationCommands} from a {@link StreamInput}
     * @param in {@link StreamInput} to read from
     * @return {@link AllocationCommands} read
     *
     * @throws IOException if something happens during read
     */
    public static AllocationCommands readFrom(StreamInput in) throws IOException {
        AllocationCommands commands = new AllocationCommands();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            commands.add(in.readNamedWriteable(AllocationCommand.class));
        }
        return commands;
    }

    /**
     * Writes {@link AllocationCommands} to a {@link StreamOutput}
     *
     * @param commands Commands to write
     * @param out {@link StreamOutput} to write the commands to
     * @throws IOException if something happens during write
     */
    public static void writeTo(AllocationCommands commands, StreamOutput out) throws IOException {
        out.writeVInt(commands.commands.size());
        for (AllocationCommand command : commands.commands) {
            out.writeNamedWriteable(command);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AllocationCommands other = (AllocationCommands) obj;
        // Override equals and hashCode for testing
        return Objects.equals(commands, other.commands);
    }

    @Override
    public int hashCode() {
        // Override equals and hashCode for testing
        return Objects.hashCode(commands);
    }

    @Override
    public String toString() {
        return "AllocationCommands{" +
               "commands=" + commands +
               '}';
    }
}
