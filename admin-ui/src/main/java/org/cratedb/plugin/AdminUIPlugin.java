package org.cratedb.plugin;


import org.cratedb.rest.action.admin.AdminUIFrontpageAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

/**
 * Crate Admin-UI Plugin
 *
 */
public class AdminUIPlugin extends AbstractPlugin {

    private final Settings settings;

    public AdminUIPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String name() {
        return "admin-ui";
    }

    @Override
    public String description() {
        return "Crate Admin-UI Plugin";
    }

    public void onModule(RestModule restModule) {
        restModule.addRestAction(AdminUIFrontpageAction.class);
    }

}
