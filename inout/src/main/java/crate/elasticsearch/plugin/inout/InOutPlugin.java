package crate.elasticsearch.plugin.inout;

import java.util.Collection;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

import crate.elasticsearch.module.dump.DumpModule;
import crate.elasticsearch.module.export.ExportModule;
import crate.elasticsearch.module.import_.ImportModule;
import crate.elasticsearch.module.reindex.ReindexModule;
import crate.elasticsearch.module.restore.RestoreModule;
import crate.elasticsearch.module.searchinto.SearchIntoModule;
import crate.elasticsearch.rest.action.admin.dump.RestDumpAction;
import crate.elasticsearch.rest.action.admin.export.RestExportAction;
import crate.elasticsearch.rest.action.admin.import_.RestImportAction;
import crate.elasticsearch.rest.action.admin.reindex.RestReindexAction;
import crate.elasticsearch.rest.action.admin.restore.RestRestoreAction;
import crate.elasticsearch.rest.action.admin.searchinto.RestSearchIntoAction;

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
