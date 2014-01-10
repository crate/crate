package io.crate.udc.service;

import io.crate.udc.ping.Pinger;
import io.crate.udc.plugin.UDCPlugin;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Timer;

public class UDCService extends AbstractLifecycleComponent<UDCService> {

    private Timer timer;
    private final Pinger pinger;

    @Inject
    public UDCService(Settings settings, Pinger pinger) {
        super(settings);
        this.pinger = pinger;
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        timer = new Timer("crate-udc");
        TimeValue initialDelay = settings.getAsTime(UDCPlugin.INITIAL_DELAY_SETTING_NAME, UDCPlugin.INITIAL_DELAY_DEFAULT_SETTING);
        TimeValue interval = settings.getAsTime(UDCPlugin.INTERVAL_SETTING_NAME, UDCPlugin.INTERVAL_DEFAULT_SETTING);
        timer.scheduleAtFixedRate(this.pinger, initialDelay.millis(), interval.millis());
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        timer.cancel();
    }

    @Override
    protected void doClose() throws ElasticSearchException {

    }
}
