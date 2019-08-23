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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

/**
 * The file watcher checks for changes on registered files
 * and notifies their listeners accordingly.
 */
class FilesWatcher implements Runnable, Closeable {

    private static final Logger LOGGER = LogManager.getLogger(FilesWatcher.class);

    private final WatchService watchService;
    private final Map<WatchKey, List<Watcher>> watchers = new HashMap<>();

    FilesWatcher() throws IOException {
        this.watchService = FileSystems.getDefault().newWatchService();
    }

    void addListener(Path path,
                     Consumer<WatchEvent<Path>> listener,
                     WatchEvent.Kind<?>[] events,
                     WatchEvent.Modifier... modifiers) throws IOException {
        Watcher watcher = new Watcher(path, listener);
        WatchKey key = watcher.registrablePath().register(watchService, events, modifiers);

        if (watchers.containsKey(key)) {
            watchers.get(key).add(watcher);
        } else {
            watchers.put(key, new ArrayList<>(Collections.singletonList(watcher)));
        }
    }

    @Override
    public void run() {
        poll();
    }

    /**
     * Polls for watch events on the registered paths.
     */
    private void poll() {
        // break the loop if no watch keys are present in the queue
        // or watch key events were consumed
        while (true) {
            WatchKey key = watchService.poll();
            if (key == null || !watchers.containsKey(key)) {
                break;
            }

            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();
                if (OVERFLOW == kind) {
                    continue;
                }
                //noinspection unchecked
                WatchEvent<Path> watchEvent = (WatchEvent<Path>) event;
                Path modifiedPath = watchEvent.context();

                for (var watcher : watchers.get(key)) {
                    if (watcher.path.getFileName().equals(modifiedPath.getFileName())) {
                        LOGGER.info("Monitored file [{}] is modified.", watcher.path);
                        watcher.listener.accept(watchEvent);
                    }
                }

                boolean valid = key.reset();
                if (!valid) {
                    break;
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        for (WatchKey key : watchers.keySet()) {
            key.cancel();
        }
        watchers.clear();
        watchService.close();
    }

    private static class Watcher {
        final Consumer<WatchEvent<Path>> listener;
        final Path path;

        private Watcher(Path path, Consumer<WatchEvent<Path>> listener) {
            this.path = path;
            this.listener = listener;
        }

        /**
         * Returns a path that can be registered with the watch service.
         * <p>
         * Only directories can be registered with the watch service.
         */
        Path registrablePath() {
            if (Files.isDirectory(path)) {
                return path;
            } else {
                return path.getParent();
            }
        }
    }
}
