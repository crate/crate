package io.crate.udc.service;

import io.crate.udc.ping.PingTask;
import io.crate.udc.plugin.UDCPlugin;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.http.HttpServerTransport;

import java.util.Timer;

public class UDCService extends AbstractLifecycleComponent<UDCService> {

    private Timer timer;

    private final ClusterService clusterService;
    private final HttpServerTransport httpServerTransport;

    @Inject
    public UDCService(Settings settings, ClusterService clusterService, HttpServerTransport httpServerTransport) {
        super(settings);
        this.clusterService = clusterService;
        this.httpServerTransport = httpServerTransport;
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        String url = settings.get(UDCPlugin.URL_SETTING_NAME, UDCPlugin.URL_DEFAULT_SETTING);
        TimeValue initialDelay = settings.getAsTime(UDCPlugin.INITIAL_DELAY_SETTING_NAME, UDCPlugin.INITIAL_DELAY_DEFAULT_SETTING);
        TimeValue interval = settings.getAsTime(UDCPlugin.INTERVAL_SETTING_NAME, UDCPlugin.INTERVAL_DEFAULT_SETTING);

        PingTask pingTask = new PingTask(this.clusterService, this.httpServerTransport, url);

        timer = new Timer("crate-udc");
        timer.scheduleAtFixedRate(pingTask, initialDelay.millis(), interval.millis());
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        timer.cancel();
    }

    @Override
    protected void doClose() throws ElasticSearchException {
        timer.cancel(); // safety net, in case of unlikely weirdness
    }
}
