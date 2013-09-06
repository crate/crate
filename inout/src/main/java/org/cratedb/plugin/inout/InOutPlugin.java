package org.cratedb.plugin.inout;

import java.util.Collection;

import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

import org.cratedb.module.dump.DumpModule;
import org.cratedb.module.export.ExportModule;
import org.cratedb.module.import_.ImportModule;
import org.cratedb.module.reindex.ReindexModule;
import org.cratedb.module.restore.RestoreModule;
import org.cratedb.module.searchinto.SearchIntoModule;
import org.cratedb.rest.action.admin.dump.RestDumpAction;
import org.cratedb.rest.action.admin.export.RestExportAction;
import org.cratedb.rest.action.admin.import_.RestImportAction;
import org.cratedb.rest.action.admin.reindex.RestReindexAction;
import org.cratedb.rest.action.admin.restore.RestRestoreAction;
import org.cratedb.rest.action.admin.searchinto.RestSearchIntoAction;

public class InOutPlugin extends AbstractPlugin {

    private final Settings settings;

    public InOutPlugin(Settings settings) {
        this.settings = settings;
    }

    public String name() {
        return "inout";
    }

    public String description() {
        return "InOut plugin";
    }

    public void onModule(RestModule restModule) {
        restModule.addRestAction(RestExportAction.class);
        restModule.addRestAction(RestImportAction.class);
        restModule.addRestAction(RestSearchIntoAction.class);
        restModule.addRestAction(RestDumpAction.class);
        restModule.addRestAction(RestRestoreAction.class);
        restModule.addRestAction(RestReindexAction.class);
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = Lists.newArrayList();
        if (!settings.getAsBoolean("node.client", false)) {
            modules.add(ExportModule.class);
            modules.add(ImportModule.class);
            modules.add(SearchIntoModule.class);
            modules.add(DumpModule.class);
            modules.add(RestoreModule.class);
            modules.add(ReindexModule.class);
        }
        return modules;
    }
}
