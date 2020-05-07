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

package org.elasticsearch.monitor.process;

import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.monitor.Probes;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;

import static org.elasticsearch.monitor.jvm.JvmInfo.jvmInfo;

public class ProcessProbe {

    private static final OperatingSystemMXBean OS_MX_BEAN = ManagementFactory.getOperatingSystemMXBean();

    private static final Method GET_MAX_FILE_DESCRIPTOR_COUNT_FIELD;
    private static final Method GET_OPEN_FILE_DESCRIPTOR_COUNT_FIELD;
    private static final Method GET_PROCESS_CPU_LOAD;
    private static final Method GET_PROCESS_CPU_TIME;
    private static final Method GET_COMMITTED_VIRTUAL_MEMORY_SIZE;

    static {
        GET_MAX_FILE_DESCRIPTOR_COUNT_FIELD = getUnixMethod("getMaxFileDescriptorCount");
        GET_OPEN_FILE_DESCRIPTOR_COUNT_FIELD = getUnixMethod("getOpenFileDescriptorCount");
        GET_PROCESS_CPU_LOAD = getMethod("getProcessCpuLoad");
        GET_PROCESS_CPU_TIME = getMethod("getProcessCpuTime");
        GET_COMMITTED_VIRTUAL_MEMORY_SIZE = getMethod("getCommittedVirtualMemorySize");
    }

    private static class ProcessProbeHolder {
        private static final ProcessProbe INSTANCE = new ProcessProbe();
    }

    public static ProcessProbe getInstance() {
        return ProcessProbeHolder.INSTANCE;
    }

    private ProcessProbe() {
    }

    /**
     * Returns the maximum number of file descriptors allowed on the system, or -1 if not supported.
     */
    public long getMaxFileDescriptorCount() {
        if (GET_MAX_FILE_DESCRIPTOR_COUNT_FIELD == null) {
            return -1;
        }
        try {
            return (Long) GET_MAX_FILE_DESCRIPTOR_COUNT_FIELD.invoke(OS_MX_BEAN);
        } catch (Exception t) {
            return -1;
        }
    }

    /**
     * Returns the number of opened file descriptors associated with the current process, or -1 if not supported.
     */
    public long getOpenFileDescriptorCount() {
        if (GET_OPEN_FILE_DESCRIPTOR_COUNT_FIELD == null) {
            return -1;
        }
        try {
            return (Long) GET_OPEN_FILE_DESCRIPTOR_COUNT_FIELD.invoke(OS_MX_BEAN);
        } catch (Exception t) {
            return -1;
        }
    }

    /**
     * Returns the process CPU usage in percent
     */
    public short getProcessCpuPercent() {
        return Probes.getLoadAndScaleToPercent(GET_PROCESS_CPU_LOAD, OS_MX_BEAN);
    }

    /**
     * Returns the CPU time (in milliseconds) used by the process on which the Java virtual machine is running, or -1 if not supported.
     */
    public long getProcessCpuTotalTime() {
        if (GET_PROCESS_CPU_TIME != null) {
            try {
                long time = (long) GET_PROCESS_CPU_TIME.invoke(OS_MX_BEAN);
                if (time >= 0) {
                    return (time / 1_000_000L);
                }
            } catch (Exception t) {
                return -1;
            }
        }
        return -1;
    }

    /**
     * Returns the size (in bytes) of virtual memory that is guaranteed to be available to the running process
     */
    public long getTotalVirtualMemorySize() {
        if (GET_COMMITTED_VIRTUAL_MEMORY_SIZE != null) {
            try {
                long virtual = (long) GET_COMMITTED_VIRTUAL_MEMORY_SIZE.invoke(OS_MX_BEAN);
                if (virtual >= 0) {
                    return virtual;
                }
            } catch (Exception t) {
                return -1;
            }
        }
        return -1;
    }

    public ProcessInfo processInfo(long refreshInterval) {
        return new ProcessInfo(jvmInfo().pid(), BootstrapInfo.isMemoryLocked(), refreshInterval);
    }

    public ProcessStats processStats() {
        ProcessStats.Cpu cpu = new ProcessStats.Cpu(getProcessCpuPercent(), getProcessCpuTotalTime());
        ProcessStats.Mem mem = new ProcessStats.Mem(getTotalVirtualMemorySize());
        return new ProcessStats(System.currentTimeMillis(), getOpenFileDescriptorCount(), getMaxFileDescriptorCount(), cpu, mem);
    }

    /**
     * Returns a given method of the OperatingSystemMXBean,
     * or null if the method is not found or unavailable.
     */
    private static Method getMethod(String methodName) {
        try {
            return Class.forName("com.sun.management.OperatingSystemMXBean").getMethod(methodName);
        } catch (Exception t) {
            // not available
            return null;
        }
    }

    /**
     * Returns a given method of the UnixOperatingSystemMXBean,
     * or null if the method is not found or unavailable.
     */
    private static Method getUnixMethod(String methodName) {
        try {
            return Class.forName("com.sun.management.UnixOperatingSystemMXBean").getMethod(methodName);
        } catch (Exception t) {
            // not available
            return null;
        }
    }
}
