package org.cratedb.plugin.crate;


import org.cratedb.rest.action.admin.CrateFrontpageAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

/**
 * Crate Defaults Plugin sets some default crate settings:
 *
 * <ul>
 * <li>Default cluster name "crate"</li>
 * <li>Delete all indices is disabled</li>
 * </ul>
 */
public class CratePlugin extends AbstractPlugin {

    private final Settings settings;

    public CratePlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Settings additionalSettings() {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();

        // Forbid DELETE on '/' aka. deleting all indices
        settingsBuilder.put("action.disable_delete_all_indices", true);

        // Set the default cluster name if not explicitly defined
        if (this.settings.get(ClusterName.SETTING).equals(ClusterName.DEFAULT.value())) {
            settingsBuilder.put("cluster.name", "crate");
        }

        return settingsBuilder.build();
    }

    @Override
    public String name() {
        return "crate";
    }

    @Override
    public String description() {
        return "Crate defaults Plugin";
    }

    public void onModule(RestModule restModule) {
        restModule.addRestAction(CrateFrontpageAction.class);
    }

}
