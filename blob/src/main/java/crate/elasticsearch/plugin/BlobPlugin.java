package crate.elasticsearch.plugin;


import com.google.common.collect.Lists;
import crate.elasticsearch.blob.BlobModule;
import crate.elasticsearch.blob.BlobService;
import crate.elasticsearch.blob.rest.RestBlobIndicesStatsAction;
import crate.elasticsearch.blob.stats.BlobStatsModule;
import crate.elasticsearch.blob.v2.BlobIndexModule;
import crate.elasticsearch.blob.v2.BlobIndicesModule;
import crate.elasticsearch.blob.v2.BlobShardModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

import java.util.Collection;

public class BlobPlugin extends AbstractPlugin {

    private final Settings settings;

    public BlobPlugin(Settings settings) {
        this.settings = settings;
    }

    public String name() {
        return "blob";
    }

    public String description() {
        return "plugin that adds BlOB support to crate";
    }

    @Override
    public Settings additionalSettings() {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        settingsBuilder.put("http.type",
                "crate.elasticsearch.http.netty.NettyHttpServerTransportModule");
        return settingsBuilder.build();
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = Lists.newArrayList();
        modules.add(BlobModule.class);
        modules.add(BlobIndicesModule.class);
        modules.add(BlobStatsModule.class);
        return modules;
    }

    public void onModule(RestModule restModule) {
        restModule.addRestAction(RestBlobIndicesStatsAction.class);
    }


    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        // only start the service if we have a data node
        if (!settings.getAsBoolean("node.client", false)) {
            Collection<Class<? extends LifecycleComponent>> services = Lists.newArrayList();
            services.add(BlobService.class);
            return services;
        }
        return super.services();
    }

    @Override
    public Collection<Class<? extends Module>> indexModules() {
        Collection<Class<? extends Module>> modules = Lists.newArrayList();
        modules.add(BlobIndexModule.class);
        return modules;
    }

    @Override
    public Collection<Class<? extends Module>> shardModules() {
        Collection<Class<? extends Module>> modules = Lists.newArrayList();
        modules.add(BlobShardModule.class);
        return modules;
    }

}