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

package io.crate.analyze.copy;

import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.jetbrains.annotations.Nullable;

public class NodeFilters implements Predicate<DiscoveryNode> {

    public static final String NAME = "node_filters";
    private final java.util.function.Predicate<DiscoveryNode> innerPredicate;

    public static NodeFilters fromMap(Map<?, ?> map) {
        String name = stringOrIllegalArgument(map, "name");
        String id = stringOrIllegalArgument(map, "id");
        if (!map.isEmpty()) {
            throw new IllegalArgumentException("Invalid node_filters arguments: " + map.keySet());
        }
        return new NodeFilters(name, id);
    }

    private static String stringOrIllegalArgument(Map<?, ?> map, String key) {
        Object obj = map.remove(key);
        try {
            return (String) obj;
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "%s argument '%s' must be a String, not %s (%s)", NAME, key, obj, obj.getClass().getSimpleName()));
        }
    }

    NodeFilters(@Nullable String name, @Nullable String id) {
        if (name == null && id == null) {
            innerPredicate = discoveryNode -> true;
        } else {
            Predicate<DiscoveryNode> namesPredicate =
                name == null ? discoveryNode -> true : new NamesPredicate(name);
            Predicate<DiscoveryNode> idsPredicate =
                id == null ? discoveryNode -> true : new IdsPredicate(id);
            innerPredicate = namesPredicate.and(idsPredicate);
        }
    }

    @Override
    public boolean test(@Nullable DiscoveryNode discoveryNode) {
        return innerPredicate.test(discoveryNode);
    }

    private static class NamesPredicate implements Predicate<DiscoveryNode> {
        private final Pattern name;

        private NamesPredicate(String name) {
            this.name = Pattern.compile(name);
        }

        @Override
        public boolean test(@Nullable DiscoveryNode discoveryNode) {
            return discoveryNode != null && name.matcher(discoveryNode.getName()).matches();
        }
    }

    private static class IdsPredicate implements Predicate<DiscoveryNode> {

        private final Pattern id;

        private IdsPredicate(String id) {
            this.id = Pattern.compile(id);
        }

        @Override
        public boolean test(@Nullable DiscoveryNode discoveryNode) {
            return discoveryNode != null && id.matcher(discoveryNode.getId()).matches();
        }
    }
}
