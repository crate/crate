package crate.elasticsearch.plugin.crate;


import crate.elasticsearch.rest.action.admin.CrateFrontpageAction;
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
        settingsBuilder.put("action.disable_delete_all_indices", true);
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
