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

package org.elasticsearch.threadpool;

import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

public class TestThreadPool extends ThreadPool {

    public TestThreadPool(String name) {
        this(name, Settings.EMPTY);
    }

    public TestThreadPool(String name, Settings settings) {
        super(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), name).put(settings).build());
    }

    public static void dumpThreads() {
        Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
        for (var entry : allStackTraces.entrySet()) {
            Thread thread = entry.getKey();

            // Exclude self; `dumpThreads` is usually used in tests and the
            // thread running the test case is typically not interesting. It
            // would only show `dumpThreads` itself
            if (thread == Thread.currentThread()) {
                continue;
            }

            StackTraceElement[] stackTraces = entry.getValue();
            if (isIdleThread(stackTraces)) {
                continue;
            }
            System.err.println("Thread: " + thread.getName());
            for (var stackTrace : stackTraces) {
                System.err.println("    " + stackTrace.toString());
            }
        }
    }

    /// @return true for threads which appear idle/uninteresting as they're waiting for work via epoll/queue
    private static boolean isIdleThread(StackTraceElement[] stackTraces) {
        if (stackTraces.length < 3) {
            return false;
        }
        StackTraceElement first = stackTraces[0];
        StackTraceElement second = stackTraces[1];
        StackTraceElement third = stackTraces[2];


        if (isParked(first, second)) {
            // java.base@24.0.2/jdk.internal.misc.Unsafe.park(Native Method)
            // java.base@24.0.2/java.util.concurrent.locks.LockSupport.park(LockSupport.java:369)
            // java.base@24.0.2/java.util.concurrent.LinkedTransferQueue$DualNode.await(LinkedTransferQueue.java:458)
            if ("java.util.concurrent.LinkedTransferQueue$DualNode".equals(third.getClassName())
                    && "await".equals(third.getMethodName())) {
                return true;
            }

            // java.base@24.0.2/jdk.internal.misc.Unsafe.park(Native Method)
            // java.base@24.0.2/java.util.concurrent.locks.LockSupport.parkNanos(LockSupport.java:271)
            // java.base@24.0.2/java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.awaitNanos(AbstractQueuedSynchronizer.java:1802)

            if ("java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject".equals(third.getClassName())
                    && "awaitNanos".equals(third.getMethodName())) {
                return true;
            }

            // java.base@24.0.2/jdk.internal.misc.Unsafe.park(Native Method)
            // java.base@24.0.2/java.util.concurrent.locks.LockSupport.park(LockSupport.java:369)
            // java.base@24.0.2/java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionNode.block(AbstractQueuedSynchronizer.java:519)
            if ("java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionNode".equals(third.getClassName())
                    && "block".equals(third.getMethodName())) {
                return true;
            }
        }

        // app//io.netty.channel.epoll.Native.epollWait0(Native Method)
        // app//io.netty.channel.epoll.Native.epollWait(Native.java:193)
        // app//io.netty.channel.epoll.EpollIoHandler.epollWait(EpollIoHandler.java:362)
        if ("io.netty.channel.epoll.Native".equals(first.getClassName())
                && "epollWait0".equals(first.getMethodName())
                && "io.netty.channel.epoll.Native".equals(second.getClassName())
                && "epollWait".equals(second.getMethodName())
                && "io.netty.channel.epoll.EpollIoHandler".equals(third.getClassName())
                && "epollWait".equals(third.getMethodName())) {
            return true;
        }
        return false;
    }

    private static boolean isParked(StackTraceElement first, StackTraceElement second) {
        return "jdk.internal.misc.Unsafe".equals(first.getClassName())
                && "park".equals(first.getMethodName())
                && first.isNativeMethod()
                && "java.util.concurrent.locks.LockSupport".equals(second.getClassName())
                && ("park".equals(second.getMethodName()) || "parkNanos".equals(second.getMethodName()));
    }
}
