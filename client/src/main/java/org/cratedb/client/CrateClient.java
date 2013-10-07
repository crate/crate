package org.cratedb.client;

import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.cluster.ClusterNameModule;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

public class CrateClient {

    private final Environment environment;
    private final Settings settings;
    private final Injector injector;
    private final InternalCrateClient internalClient;


    public CrateClient(Settings pSettings, boolean loadConfigSettings) throws
            ElasticSearchException {
        ImmutableSettings.Builder settingsBuilder = settingsBuilder();

        Tuple<Settings, Environment> tuple = InternalSettingsPreparer.prepareSettings(
            pSettings, loadConfigSettings);
        Settings settings = settingsBuilder().put(tuple.v1())
                .put("network.server", false)
                .put("node.client", true)
                .put("client.transport.ignore_cluster_name", true)
                .build();
        this.environment = tuple.v2();

        this.settings = settings;
        Version version = Version.CURRENT;

        CompressorFactory.configure(this.settings);

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new CrateClientModule());
        modules.add(new Version.Module(version));

        modules.add(new SettingsModule(this.settings));

        modules.add(new ClusterNameModule(this.settings));
        modules.add(new TransportModule(this.settings));

        injector = modules.createInjector();
        injector.getInstance(TransportService.class).start();
        internalClient = injector.getInstance(InternalCrateClient.class);
    }

    public CrateClient() {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS, true);
    }

    public CrateClient(String... servers) {
        this();
        for (String server : servers) {
            String[] parts = server.split(":");
            String host = parts[0];
            Integer port = 9300;
            if (parts.length == 2) {
                port = Integer.parseInt(parts[1]);
            }
            internalClient.addTransportAddress(new InetSocketTransportAddress(host, port));
        }
    }

    public ActionFuture<SQLResponse> sql(String stmt) {
        return sql(new SQLRequest(stmt));
    }

    public ActionFuture<SQLResponse> sql(SQLRequest request) {
        return internalClient.sql(request);
    }


}
