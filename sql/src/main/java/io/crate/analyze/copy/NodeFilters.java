/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.copy;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.elasticsearch.cluster.node.DiscoveryNode;

import javax.annotation.Nullable;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

public class NodeFilters implements Predicate<DiscoveryNode> {

    public static final String NAME = "node_filters";
    private final Predicate<DiscoveryNode> innerPredicate;

    public static NodeFilters fromMap(Map map) {
        String name = stringOrIllegalArgument(map, "name");
        String id = stringOrIllegalArgument(map, "id");
        if (!map.isEmpty()) {
            throw new IllegalArgumentException("Invalid node_filters arguments: " + map.keySet());
        }
        return new NodeFilters(name, id);
    }

    private static String stringOrIllegalArgument(Map map, String key) {
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
            innerPredicate = Predicates.alwaysTrue();
        } else {
            Predicate<DiscoveryNode> namesPredicate =
                name == null ? Predicates.<DiscoveryNode>alwaysTrue() : new NamesPredicate(name);
            Predicate<DiscoveryNode> idsPredicate =
                id == null ? Predicates.<DiscoveryNode>alwaysTrue() : new IdsPredicate(id);
            innerPredicate = Predicates.and(namesPredicate, idsPredicate);
        }
    }

    @Override
    public boolean apply(@Nullable DiscoveryNode input) {
        return innerPredicate.apply(input);
    }


    private static class NamesPredicate implements Predicate<DiscoveryNode> {
        private final Pattern name;

        public NamesPredicate(String name) {
            this.name = Pattern.compile(name);
        }

        @Override
        public boolean apply(@Nullable DiscoveryNode input) {
            return input != null && name.matcher(input.getName()).matches();
        }
    }

    private static class IdsPredicate implements Predicate<DiscoveryNode> {

        private final Pattern id;

        public IdsPredicate(String id) {
            this.id = Pattern.compile(id);
        }

        @Override
        public boolean apply(@Nullable DiscoveryNode input) {
            return input != null && id.matcher(input.getId()).matches();
        }
    }
}
