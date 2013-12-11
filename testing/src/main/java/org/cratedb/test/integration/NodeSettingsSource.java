package org.cratedb.test.integration;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

public abstract class NodeSettingsSource {

    public static final NodeSettingsSource EMPTY = new NodeSettingsSource() {
        @Override
        public Settings settings(int nodeOrdinal) {
            return null;
        }
    };

    /**
     * @return  the settings for the node represented by the given ordinal, or {@code null} if there are not settings defined (in which
     *          case a random settings will be generated for the node)
     */
    public abstract Settings settings(int nodeOrdinal);

    public static class Immutable extends NodeSettingsSource {

        private final Map<Integer, Settings> settingsPerNode;

        private Immutable(Map<Integer, Settings> settingsPerNode) {
            this.settingsPerNode = settingsPerNode;
        }

        public static Builder builder() {
            return new Builder();
        }

        @Override
        public Settings settings(int nodeOrdinal) {
            return settingsPerNode.get(nodeOrdinal);
        }

        public static class Builder {

            private final ImmutableMap.Builder<Integer, Settings> settingsPerNode = ImmutableMap.builder();

            private Builder() {
            }

            public Builder set(int ordinal, Settings settings) {
                settingsPerNode.put(ordinal, settings);
                return this;
            }

            public Immutable build() {
                return new Immutable(settingsPerNode.build());
            }

        }
    }
}