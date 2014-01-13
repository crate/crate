package io.crate.udc.plugin;

import io.crate.udc.service.UDCService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.AbstractPlugin;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;

public class UDCPlugin extends AbstractPlugin {

    public static final String ENABLED_SETTING_NAME = "udc.enabled";
    public static final boolean ENABLED_DEFAULT_SETTING = true;

    public static final String INITIAL_DELAY_SETTING_NAME = "udc.initial_delay";
    public static final TimeValue INITIAL_DELAY_DEFAULT_SETTING = new TimeValue(10, TimeUnit.MINUTES);

    public static final String INTERVAL_SETTING_NAME = "udc.interval";
    public static final TimeValue INTERVAL_DEFAULT_SETTING = new TimeValue(24, TimeUnit.HOURS);

    public static final String URL_SETTING_NAME = "udc.url";
    public static final String URL_DEFAULT_SETTING = "https://udc.crate.io/";

    private final Settings settings;

    public UDCPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        if (!settings.getAsBoolean("node.client", false)
                && settings.getAsBoolean(ENABLED_SETTING_NAME, ENABLED_DEFAULT_SETTING)) {
            Collection<Class<? extends LifecycleComponent>> services = newArrayList();
            services.add(UDCService.class);
            return services;
        }
        return super.services();
    }

    @Override
    public String name() {
        return "udc";
    }

    @Override
    public String description() {
        return "crate plugin for Usage Data Collection (UDC)";
    }
}
