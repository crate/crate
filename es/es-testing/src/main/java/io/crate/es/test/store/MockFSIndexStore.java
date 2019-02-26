/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.es.test.store;

import org.apache.logging.log4j.Logger;
import io.crate.es.common.Nullable;
import io.crate.es.common.logging.Loggers;
import io.crate.es.common.settings.Setting;
import io.crate.es.common.settings.Setting.Property;
import io.crate.es.common.settings.Settings;
import io.crate.es.index.IndexModule;
import io.crate.es.index.IndexSettings;
import io.crate.es.index.shard.IndexEventListener;
import io.crate.es.index.shard.IndexShard;
import io.crate.es.index.shard.IndexShardState;
import io.crate.es.index.shard.ShardId;
import io.crate.es.index.shard.ShardPath;
import io.crate.es.index.store.DirectoryService;
import io.crate.es.index.store.IndexStore;
import io.crate.es.plugins.IndexStorePlugin;
import io.crate.es.plugins.Plugin;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class MockFSIndexStore extends IndexStore {

    public static final Setting<Boolean> INDEX_CHECK_INDEX_ON_CLOSE_SETTING =
        Setting.boolSetting("index.store.mock.check_index_on_close", true, Property.IndexScope, Property.NodeScope);

    public static class TestPlugin extends Plugin implements IndexStorePlugin {
        @Override
        public Settings additionalSettings() {
            return Settings.builder().put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "mock").build();
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(INDEX_CHECK_INDEX_ON_CLOSE_SETTING,
            MockFSDirectoryService.CRASH_INDEX_SETTING,
            MockFSDirectoryService.RANDOM_IO_EXCEPTION_RATE_SETTING,
            MockFSDirectoryService.RANDOM_PREVENT_DOUBLE_WRITE_SETTING,
            MockFSDirectoryService.RANDOM_NO_DELETE_OPEN_FILE_SETTING,
            MockFSDirectoryService.RANDOM_IO_EXCEPTION_RATE_ON_OPEN_SETTING);
        }

        @Override
        public Map<String, Function<IndexSettings, IndexStore>> getIndexStoreFactories() {
            return Collections.singletonMap("mock", MockFSIndexStore::new);
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            Settings indexSettings = indexModule.getSettings();
            if ("mock".equals(indexSettings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey()))) {
                if (INDEX_CHECK_INDEX_ON_CLOSE_SETTING.get(indexSettings)) {
                    indexModule.addIndexEventListener(new Listener());
                }
            }
        }
    }

    MockFSIndexStore(IndexSettings indexSettings) {
        super(indexSettings);
    }

    public DirectoryService newDirectoryService(ShardPath path) {
        return new MockFSDirectoryService(indexSettings, this, path);
    }

    private static final EnumSet<IndexShardState> validCheckIndexStates = EnumSet.of(
            IndexShardState.STARTED, IndexShardState.POST_RECOVERY
    );
    private static final class Listener implements IndexEventListener {

        private final Map<IndexShard, Boolean> shardSet = Collections.synchronizedMap(new IdentityHashMap<>());
        @Override
        public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
            if (indexShard != null) {
                Boolean remove = shardSet.remove(indexShard);
                if (remove == Boolean.TRUE) {
                    Logger logger = Loggers.getLogger(getClass(), indexShard.shardId());
                    MockFSDirectoryService.checkIndex(logger, indexShard.store(), indexShard.shardId());
                }
            }
        }

        @Override
        public void indexShardStateChanged(IndexShard indexShard, @Nullable IndexShardState previousState,
                IndexShardState currentState, @Nullable String reason) {
            if (currentState == IndexShardState.CLOSED && validCheckIndexStates.contains(previousState)) {
               shardSet.put(indexShard, Boolean.TRUE);
            }

        }
    }

}
