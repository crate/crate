/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A mock task manager that allows adding listeners for events
 */
public class MockTaskManager extends TaskManager {

    private static final Logger logger = LogManager.getLogger(MockTaskManager.class);

    public static final Setting<Boolean> USE_MOCK_TASK_MANAGER_SETTING =
        Setting.boolSetting("tests.mock.taskmanager.enabled", false, Property.NodeScope);

    private final Collection<MockTaskManagerListener> listeners = new CopyOnWriteArrayList<>();

    public MockTaskManager(Settings settings, ThreadPool threadPool, Set<String> taskHeaders) {
        super(settings, threadPool, taskHeaders);
    }

    @Override
    public Task register(String type, String action, TaskAwareRequest request) {
        Task task = super.register(type, action, request);
        for (MockTaskManagerListener listener : listeners) {
            try {
                listener.onTaskRegistered(task);
            } catch (Exception e) {
                logger.warn(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "failed to notify task manager listener about registering the task with id {}",
                        task.getId()),
                    e);
            }
        }
        return task;
    }

    @Override
    public Task unregister(Task task) {
        Task removedTask = super.unregister(task);
        if (removedTask != null) {
            for (MockTaskManagerListener listener : listeners) {
                try {
                    listener.onTaskUnregistered(task);
                } catch (Exception e) {
                    logger.warn(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "failed to notify task manager listener about unregistering the task with id {}", task.getId()), e);
                }
            }
        } else {
            logger.warn("trying to remove the same with id {} twice", task.getId());
        }
        return removedTask;
    }

    @Override
    public void waitForTaskCompletion(Task task, long untilInNanos) {
        for (MockTaskManagerListener listener : listeners) {
            try {
                listener.waitForTaskCompletion(task);
            } catch (Exception e) {
                logger.warn(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "failed to notify task manager listener about waitForTaskCompletion the task with id {}",
                        task.getId()),
                    e);
            }
        }
        super.waitForTaskCompletion(task, untilInNanos);
    }

    public void addListener(MockTaskManagerListener listener) {
        listeners.add(listener);
    }

    public void removeListener(MockTaskManagerListener listener) {
        listeners.remove(listener);
    }
}
