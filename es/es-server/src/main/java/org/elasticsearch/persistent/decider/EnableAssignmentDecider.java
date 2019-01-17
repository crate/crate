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
package org.elasticsearch.persistent.decider;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.Locale;

import static org.elasticsearch.common.settings.Setting.Property.Dynamic;
import static org.elasticsearch.common.settings.Setting.Property.NodeScope;

/**
 * {@link EnableAssignmentDecider} is used to allow/disallow the persistent tasks
 * to be assigned to cluster nodes.
 * <p>
 * Allocation settings can have the following values (non-casesensitive):
 * <ul>
 *     <li> <code>NONE</code> - no persistent tasks can be assigned
 *     <li> <code>ALL</code> - all persistent tasks can be assigned to nodes
 * </ul>
 *
 * @see Allocation
 */
public class EnableAssignmentDecider {

    public static final Setting<Allocation> CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING =
        new Setting<>("cluster.persistent_tasks.allocation.enable", Allocation.ALL.toString(), Allocation::fromString, Dynamic, NodeScope);

    private volatile Allocation enableAssignment;

    public EnableAssignmentDecider(final Settings settings, final ClusterSettings clusterSettings) {
        this.enableAssignment = CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING, this::setEnableAssignment);
    }

    public void setEnableAssignment(final Allocation enableAssignment) {
        this.enableAssignment = enableAssignment;
    }

    /**
     * Returns a {@link AssignmentDecision} whether the given persistent task can be assigned
     * to a node of the cluster. The decision depends on the current value of the setting
     * {@link EnableAssignmentDecider#CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING}.
     *
     * @return the {@link AssignmentDecision}
     */
    public AssignmentDecision canAssign() {
        if (enableAssignment == Allocation.NONE) {
            return new AssignmentDecision(AssignmentDecision.Type.NO, "no persistent task assignments are allowed due to cluster settings");
        }
        return AssignmentDecision.YES;
    }

    /**
     * Allocation values or rather their string representation to be used used with
     * {@link EnableAssignmentDecider#CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING}
     * via cluster settings.
     */
    public enum Allocation {

        NONE,
        ALL;

        public static Allocation fromString(final String strValue) {
            if (strValue == null) {
                return null;
            } else {
                String value = strValue.toUpperCase(Locale.ROOT);
                try {
                    return valueOf(value);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Illegal value [" + value + "] for ["
                        + CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.getKey() + "]");
                }
            }
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
