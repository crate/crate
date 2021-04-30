/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.monitor;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.core.AnyOf;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.Is.is;

public class SysInfoUtilTest extends ESTestCase {

    private static final SysInfo.Builder SYSINFO_BUILDER = new SysInfo.Builder();
    private static final AnyOf<String> X86_64 = anyOf(is("x86_64"), is("amd64"), is("x64"));

    private void assertVendorVersionFromGenericLine(String line, String expectedVersionString) {
        SysInfo sysInfo = new SysInfo();
        SYSINFO_BUILDER.parseGenericVendorLine(sysInfo, line);
        assertEquals(sysInfo.vendorVersion(), expectedVersionString);
    }

    @Test
    public void testParseKeyValue() {
        assertThat(SysInfo.Builder.parseKeyValuePair("KEY=\"val\"ue\""), is(new String[]{"KEY", "val\"ue"}));
        assertThat(SysInfo.Builder.parseKeyValuePair("KEY=\"value\""), is(new String[]{"KEY", "value"}));
        assertThat(SysInfo.Builder.parseKeyValuePair("KEY=value"), is(new String[]{"KEY", "value"}));
        assertThat(SysInfo.Builder.parseKeyValuePair("KEY="), is(new String[]{"KEY", ""}));
        assertThat(SysInfo.Builder.parseKeyValuePair("KEY"), is(new String[]{"KEY", ""}));
        assertThat(SysInfo.Builder.parseKeyValuePair(""), is(new String[]{"", ""}));
    }

    @Test
    public void testWindows() {
        SysInfo sysInfo = new SysInfo.Builder()
            .withName("Windows 10")
            .withVersion("10.0")
            .withArch("x86_64")
            .gather();
        assertThat(sysInfo.arch(), is("x86_64"));
        assertThat(sysInfo.description(), is("Microsoft Windows 10"));
        assertThat(sysInfo.machine(), X86_64);
        assertThat(sysInfo.name(), is("Win32"));
        assertThat(sysInfo.patchLevel(), is(""));
        assertThat(sysInfo.vendor(), is("Microsoft"));
        assertThat(sysInfo.vendorCodeName(), is(""));
        assertThat(sysInfo.vendorName(), is("Windows 10"));
        assertThat(sysInfo.vendorVersion(), is("10"));
        assertThat(sysInfo.version(), is("10.0"));
    }

    @Test
    public void testMacOS() {
        SysInfo sysInfo = new SysInfo.Builder()
            .withName("Mac OS")
            .withVersion("10.12.6")
            .withArch("x86_64")
            .gather();
        assertThat(sysInfo.arch(), is("x86_64"));
        assertThat(sysInfo.description(), is("Mac OS X (Sierra)"));
        assertThat(sysInfo.machine(), X86_64);
        assertThat(sysInfo.name(), is("MacOSX"));
        assertThat(sysInfo.patchLevel(), is(""));
        assertThat(sysInfo.vendor(), is("Apple"));
        assertThat(sysInfo.vendorCodeName(), is("Sierra"));
        assertThat(sysInfo.vendorName(), is("Mac OS X"));
        assertThat(sysInfo.vendorVersion(), is("10.12"));
        assertThat(sysInfo.version(), is("10.12.6"));
    }

    @Test
    public void testDarwin() {
        SysInfo sysInfo = new SysInfo.Builder()
            .withName("Darwin")
            .withVersion("16.6.0")
            .withArch("x86_64")
            .gather();
        assertThat(sysInfo.arch(), is("x86_64"));
        assertThat(sysInfo.description(), is("Mac OS X (Sierra)"));
        assertThat(sysInfo.machine(), X86_64);
        assertThat(sysInfo.name(), is("MacOSX"));
        assertThat(sysInfo.patchLevel(), is(""));
        assertThat(sysInfo.vendor(), is("Apple"));
        assertThat(sysInfo.vendorCodeName(), is("Sierra"));
        assertThat(sysInfo.vendorName(), is("Mac OS X"));
        assertThat(sysInfo.vendorVersion(), is("16.6"));
        assertThat(sysInfo.version(), is("16.6.0"));
    }

    @Test
    public void testParseRedHatVendorCentOs() {
        SysInfo sysInfo = new SysInfo();
        String release = "CentOS Linux release 7.4.1708 (Core)";
        SYSINFO_BUILDER.parseRedHatVendorLine(sysInfo, release);
        assertThat(sysInfo.vendor(), is("CentOS"));
        assertThat(sysInfo.vendorCodeName(), is("Core"));
    }

    @Test
    public void testParseRedHatVendorRhel() {
        SysInfo sysInfo = new SysInfo();
        String release = "Red Hat Enterprise Linux Server release 6.7 (Santiago)";
        SYSINFO_BUILDER.parseGenericVendorLine(sysInfo, release);
        assertThat(sysInfo.vendorVersion(), is("6.7")); // required for parseRedHatVendorLine()
        SYSINFO_BUILDER.parseRedHatVendorLine(sysInfo, release);
        assertThat(sysInfo.vendorVersion(), is("Enterprise Linux 6"));
        assertThat(sysInfo.vendorCodeName(), is("Santiago"));
    }

    @Test
    public void testGenericVendor() {
        assertVendorVersionFromGenericLine("", "");
        assertVendorVersionFromGenericLine("8", "8");
        assertVendorVersionFromGenericLine("8.10", "8.10");
        assertVendorVersionFromGenericLine("8.10.1", "8.10.1");
        assertVendorVersionFromGenericLine("buster/sid", "");
        assertVendorVersionFromGenericLine("jessie", "");
        assertVendorVersionFromGenericLine("jessie 8.10", "8.10");
        assertVendorVersionFromGenericLine("9.1 stretch", "9.1");
        assertVendorVersionFromGenericLine("9.1.x", "9.1.");
    }

    @Test
    public void testFailedSysCall() {
        // by default sys call filter is enabled, so any Runtime.getRuntime().exec(...) will fail
        List<String> result = SysInfo.sysCall(new String[]{"undefined"}, "default");
        assertThat(result.get(0), is("default"));
    }
}
