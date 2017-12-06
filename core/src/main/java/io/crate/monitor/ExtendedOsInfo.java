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

package io.crate.monitor;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class ExtendedOsInfo {

    private final Map<String, String> kernelData;

    public ExtendedOsInfo(SysInfoUtil.SysInfo sysInfo) {
        kernelData = ImmutableMap.<String, String>builder()
            .put("Arch", sysInfo.arch())
            .put("Description", sysInfo.description())
            .put("Machine", sysInfo.machine())
            .put("Name", sysInfo.name())
            .put("PatchLevel", sysInfo.patchLevel())
            .put("Vendor", sysInfo.vendor())
            .put("VendorCodeName", sysInfo.vendorCodeName())
            .put("VendorName", sysInfo.vendorName())
            .put("VendorVersion", sysInfo.vendorVersion())
            .put("Version", sysInfo.version())
            .build();
    }

    public Map<String, String> kernelData() {
        return kernelData;
    }
}
