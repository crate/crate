/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.iothub.processor;

import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.EventProcessorOptions;
import io.crate.action.sql.SQLOperations;
import io.crate.auth.user.UserManager;
import io.crate.ingestion.IngestionService;
import io.crate.iothub.operations.EventIngestService;
import io.crate.metadata.Functions;
import io.crate.settings.CrateSetting;
import io.crate.settings.SharedSettings;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.function.Function;

@Singleton
public class EventHubProcessor extends AbstractLifecycleComponent {

    public static final CrateSetting<Boolean> IOT_HUB_ENABLED_SETTING = CrateSetting.of(
        Setting.boolSetting("ingestion.iot_hub.enabled", false, Setting.Property.NodeScope),
        DataTypes.BOOLEAN);

    public static final CrateSetting<String> CONNECTION_STRING = CrateSetting.of(
        Setting.simpleString("ingestion.iot_hub.connectionString", Setting.Property.NodeScope),
        DataTypes.STRING
    );

    public static final CrateSetting<String> STORAGE_CONTAINER_NAME = CrateSetting.of(
        Setting.simpleString("ingestion.iot_hub.storageContainerName", Setting.Property.NodeScope),
        DataTypes.STRING
    );

    public static final CrateSetting<String> STORAGE_CONNECTION_STRING = CrateSetting.of(
        Setting.simpleString("ingestion.iot_hub.storageConnectionString", Setting.Property.NodeScope),
        DataTypes.STRING
    );

    public static final CrateSetting<String> EVENT_HUB_NAME = CrateSetting.of(
        Setting.simpleString("ingestion.iot_hub.eventHubName", Setting.Property.NodeScope),
        DataTypes.STRING
    );

    public static final CrateSetting<String> CONSUMER_GROUP_NAME = CrateSetting.of(
        new Setting<>("ingestion.iot_hub.consumerGroupName", "$Default",
            Function.identity(), Setting.Property.NodeScope),
        DataTypes.STRING
    );

    private final Logger logger;
    private final String connectionString;
    private final String storageContainerName;
    private final String storageConnectionString;
    private final String eventHubName;
    private final String consumerGroupName;

    private final boolean isEnabled;
    private final boolean isEnterprise;

    private EventProcessorHost host;

    private EventIngestService eventIngestService;

    @Inject
    public EventHubProcessor(Settings settings,
                             Functions functions,
                             SQLOperations sqlOperations,
                             UserManager userManager,
                             IngestionService ingestionService
    ) {
        super(settings);
        logger = Loggers.getLogger(EventHubProcessor.class, settings);
        isEnabled = IOT_HUB_ENABLED_SETTING.setting().get(settings);
        isEnterprise = SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().get(settings);
        connectionString = CONNECTION_STRING.setting().get(settings);
        storageContainerName = STORAGE_CONTAINER_NAME.setting().get(settings);
        storageConnectionString = STORAGE_CONNECTION_STRING.setting().get(settings);
        eventHubName = EVENT_HUB_NAME.setting().get(settings);
        consumerGroupName = CONSUMER_GROUP_NAME.setting().get(settings);
        eventIngestService = new EventIngestService(functions, sqlOperations, userManager, ingestionService);
    }

    @Override
    protected void doStart() {
        if (!isEnterprise || !isEnabled) {
            return;
        }
        
        eventIngestService.initalize();
        host = new EventProcessorHost(
            EventProcessorHost.createHostName(this.nodeName()),
            this.eventHubName,
            this.consumerGroupName,
            this.connectionString,
            this.storageConnectionString,
            this.storageContainerName
        );

        logger.info("Registering host :: " + host.getHostName());
        EventProcessorOptions options = new EventProcessorOptions();
        options.setExceptionNotification(new ErrorNotificationHandler());

        try {
            EventProcessorFactory factory = new EventProcessorFactory(eventIngestService);
            host.registerEventProcessorFactory(factory, options)
                .whenComplete((unused, e) -> {
                    if (e != null) {
                        logger.error("Failure while registering: " + e.toString());
                        if (e.getCause() != null) {
                            logger.error("Inner exception: " + e.getCause().toString());
                        }
                    }
                })
                .get(); // Wait for everything to finish before exiting main!
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    protected void doStop() {
        host.unregisterEventProcessor();
    }

    @Override
    protected void doClose() {

    }
}
