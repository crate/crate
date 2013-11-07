package org.cratedb.node;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.ImmutableSettings;

public class NodeSettings {

    /**
     * Crate default settings
     *
     * @return
     */
    public static void applyDefaultSettings(ImmutableSettings.Builder
                                                         settingsBuilder) {
        // Forbid DELETE on '/' aka. deleting all indices
        settingsBuilder.put("action.disable_delete_all_indices", true);

        // Set the default cluster name if not explicitly defined
        if (settingsBuilder.get(ClusterName.SETTING).equals(ClusterName.DEFAULT.value())) {
            settingsBuilder.put("cluster.name", "crate");
        }

    }

}
