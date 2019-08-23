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

package io.crate.protocols.ssl;

import com.sun.nio.file.SensitivityWatchEventModifier;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

@Singleton
public class SslContextProviderService extends AbstractLifecycleComponent {

    private final SslContextProvider sslContextProvider;
    private final Settings settings;
    private final ThreadPool threadPool;

    private FilesWatcher filesWatcher;
    private Scheduler.Cancellable watchRoutine;

    @Inject
    public SslContextProviderService(Settings settings,
                                     ThreadPool threadPool,
                                     SslContextProvider sslContextProvider) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.sslContextProvider = sslContextProvider;
    }

    @Override
    protected void doStart() {
        SensitivityWatchEventModifier pollSensitivity = pollSensitivity();

        try {
            filesWatcher = new FilesWatcher();

            var keystorePath = SslConfigSettings.SSL_KEYSTORE_FILEPATH.setting().get(settings);
            if (!keystorePath.isEmpty()) {
                filesWatcher.addListener(
                    Paths.get(keystorePath),
                    event -> sslContextProvider.reloadSslContext(),
                    new WatchEvent.Kind[]{ENTRY_MODIFY},
                    pollSensitivity);
            }
            var trustStorePath = SslConfigSettings.SSL_TRUSTSTORE_FILEPATH.setting().get(settings);
            if (!trustStorePath.isEmpty()) {
                filesWatcher.addListener(
                    Paths.get(trustStorePath),
                    event -> sslContextProvider.reloadSslContext(),
                    new WatchEvent.Kind[]{ENTRY_MODIFY},
                    pollSensitivity);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        watchRoutine = threadPool.scheduleWithFixedDelay(
            filesWatcher,
            new TimeValue(pollSensitivity.sensitivityValueInSeconds(), TimeUnit.SECONDS),
            ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        if (watchRoutine != null && !watchRoutine.isCancelled()) {
            watchRoutine.cancel();
            watchRoutine = null;
        }
        IOUtils.closeWhileHandlingException(filesWatcher);
    }

    @Override
    protected void doClose() {
    }

    private SensitivityWatchEventModifier pollSensitivity() {
        TimeValue pollIntervalSetting = SslConfigSettings.SSL_RESOURCE_POLL_INTERVAL.setting().get(settings);
        long pollInSeconds = pollIntervalSetting.getSeconds();
        SensitivityWatchEventModifier pollSensitivity;
        if (pollInSeconds <= SensitivityWatchEventModifier.HIGH.sensitivityValueInSeconds()) {
            pollSensitivity = SensitivityWatchEventModifier.HIGH;
        } else if (pollInSeconds <= SensitivityWatchEventModifier.MEDIUM.sensitivityValueInSeconds()) {
            pollSensitivity = SensitivityWatchEventModifier.MEDIUM;
        } else {
            pollSensitivity = SensitivityWatchEventModifier.LOW;
        }
        return pollSensitivity;
    }
}
