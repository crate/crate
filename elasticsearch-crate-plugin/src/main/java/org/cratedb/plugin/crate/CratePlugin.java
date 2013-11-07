package org.cratedb.plugin.crate;


import org.cratedb.rest.action.admin.CrateFrontpageAction;
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
