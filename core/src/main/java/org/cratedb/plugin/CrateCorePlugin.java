package org.cratedb.plugin;

import org.cratedb.module.CrateCoreModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.cratedb.rest.CrateRestMainAction;

import java.util.ArrayList;
import java.util.Collection;

public class CrateCorePlugin extends AbstractPlugin {

    private final Settings settings;

    public CrateCorePlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String name() {
        return "crate-core";
    }

    @Override
    public String description() {
        return "plugin that provides a collection of utilities used in other crate modules.";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = new ArrayList<>();
        if (!settings.getAsBoolean("node.client", false)) {
            modules.add(CrateCoreModule.class);
        }
        return modules;
    }

    public void onModule(RestModule restModule) {
        restModule.addRestAction(CrateRestMainAction.class);
    }
}
