/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.metadata.sys;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.metadata.SystemTable.Builder;
import io.crate.metadata.SystemTable.ObjectBuilder;
import io.crate.metadata.settings.CrateSettings;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public class SysClusterTableInfo {

    private SysClusterTableInfo() {}

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "cluster");

    public record LoggerEntry(String loggerName, String level) {}

    public static SystemTable<Void> of(ClusterService clusterService) {
        Settings settings = clusterService.getSettings();
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        var relBuilder = SystemTable.<Void>builder(IDENT)
            .add("id", DataTypes.STRING, _ -> clusterService.state().metadata().clusterUUID())
            .add("name", DataTypes.STRING, _ -> ClusterName.CLUSTER_NAME_SETTING.get(settings).value())
            .add("master_node", DataTypes.STRING, _ -> clusterService.state().nodes().getMasterNodeId())
            .startObject("license", ignored -> true)
                .add("expiry_date", DataTypes.TIMESTAMPZ, ignored -> null)
                .add("issued_to", DataTypes.STRING, ignored -> null)
                .add("max_nodes", DataTypes.INTEGER, ignored -> null)
            .endObject();

        var settingsBuilder = relBuilder.startObject("settings")
            .startObjectArray("logger", extractLoggers(clusterSettings))
                .add("name", DataTypes.STRING, LoggerEntry::loggerName)
                .add("level", DataTypes.STRING, LoggerEntry::level)
            .endObjectArray();

        // turns the settings:
        //
        // [
        //  [stats, enabled],
        //  [stats, jobs_log_size],
        //  ...
        // ]
        //
        // into tree form:
        //
        //   Node
        //    name: stats
        //    children: [
        //      Leaf:
        //        name: enabled
        //        value: CrateSetting{stats.enabled}
        //      Leaf:
        //        name: jobs_log_size
        //        value: CrateSetting{stats.jobs_log_size}
        //
        //
        // To make it easier to build the objects
        var rootNode = toTree();

        for (var child : rootNode.children) {
            addSetting(clusterSettings, settingsBuilder, child);
        }
        return settingsBuilder
            .endObject()
            .build();
    }

    private static void addSetting(ClusterSettings clusterSettings,
                                   ObjectBuilder<Void, ? extends Builder<Void>> settingsBuilder,
                                   Node<Setting<?>> element) {
        if (element instanceof Leaf<?>) {
            Leaf<Setting<?>> leaf = (Leaf<Setting<?>>) element;
            var setting = leaf.value;
            var valueType = (DataType<Object>) leaf.value.dataType();
            Object settingValue = clusterSettings.get(setting);
            if (settingValue instanceof Settings groupSetting &&
                valueType.id() == ObjectType.ID) {
                settingsBuilder.addDynamicObject(leaf.name, DataTypes.STRING, _ -> groupSetting.getAsStructuredMap());
            } else {
                settingsBuilder.add(
                    leaf.name,
                    valueType,
                    _ -> valueType.implicitCast(clusterSettings.get(setting)));
            }
        } else {
            var objectSetting = settingsBuilder.startObject(element.name);
            for (var c : element.children) {
                addSetting(clusterSettings, objectSetting, c);
            }
            objectSetting.endObject();
        }
    }

    private static Function<Void, List<LoggerEntry>> extractLoggers(ClusterSettings clusterSettings) {
        return _ -> {
            ArrayList<LoggerEntry> loggers = new ArrayList<>();
            Settings loggerSettings = clusterSettings.getLoggerSettings();
            for (var settingName : loggerSettings.keySet()) {
                loggers.add(new LoggerEntry(settingName, loggerSettings.get(settingName).toUpperCase(Locale.ENGLISH)));
            }
            return loggers;
        };
    }


    private static Node<Setting<?>> toTree() {
        Node<Setting<?>> rootNode = new Node<>("root");
        for (var setting : CrateSettings.EXPOSED_SETTINGS) {
            rootNode.add(setting.path(), setting);
        }
        return rootNode;
    }

    private static class Node<T> {

        final String name;
        private final ArrayList<Node<T>> children = new ArrayList<>();

        private Node(String name) {
            this.name = name;
        }

        private void add(List<String> path, T value) {
            switch (path.size()) {
                case 0:
                    throw new IllegalArgumentException("Path must not be empty");

                case 1:
                    children.add(new Leaf<>(path.get(0), value));
                    break;

                default:
                    var valueName = path.get(0);
                    for (var child : children) {
                        if (child.name.equals(valueName)) {
                            child.add(path.subList(1, path.size()), value);
                            return;
                        }
                    }
                    Node<T> newChild = new Node<>(valueName);
                    children.add(newChild);
                    newChild.add(path.subList(1, path.size()), value);
                    break;
            }
        }
    }

    private static class Leaf<T> extends Node<T> {

        private final T value;

        private Leaf(String name, T value) {
            super(name);
            this.value = value;
        }
    }
}
