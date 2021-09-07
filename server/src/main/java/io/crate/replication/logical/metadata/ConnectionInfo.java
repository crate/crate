/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.replication.logical.metadata;

import io.crate.exceptions.InvalidArgumentException;
import io.crate.metadata.settings.Validators;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.transport.RemoteConnectionStrategy.REMOTE_CONNECTION_MODE;
import static org.elasticsearch.transport.SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS;

public class ConnectionInfo implements Writeable {

    public static final Setting<String> USERNAME = Setting.simpleString("user");

    public static final Setting<String> PASSWORD = Setting.simpleString("password");

    public static final Setting<String> SSLMODE = new Setting<>(
        "sslmode",
        "prefer",
        Function.identity(),
        Validators.stringValidator("sslmode", "prefer", "allow", "disable", "require"),
        DataTypes.STRING
    );

    private static final Set<String> SUPPORTED_SETTINGS = Set.of(
        USERNAME.getKey(),
        PASSWORD.getKey(),
        SSLMODE.getKey(),
        REMOTE_CONNECTION_MODE.getKey(),
        REMOTE_CLUSTER_SEEDS.getKey()
    );

    private static final String DEFAULT_PORT = "4300";

    public static ConnectionInfo fromURL(String url) {
        List<String> hosts = new ArrayList<>();

        String urlServer = url;
        String urlArgs = "";

        int qPos = url.indexOf('?');
        if (qPos != -1) {
            urlServer = url.substring(0, qPos);
            urlArgs = url.substring(qPos + 1);
        }

        if (!urlServer.startsWith("crate://")) {
            throw new InvalidArgumentException(
                String.format(Locale.ENGLISH,
                              "The connection string must start with \"crate://\" but was: \"%s\"", url)
            );
        }
        urlServer = urlServer.substring("crate://".length());

        int slash = urlServer.indexOf('/');
        if (slash != -1 && slash != urlServer.length()) {
            throw new InvalidArgumentException(
                String.format(Locale.ENGLISH,
                              "Database argument is not supported inside the connection string: %s", url)
            );
        }
        slash = urlServer.length();

        String[] addresses = urlServer.substring(0, slash).split(",");
        for (String address : addresses) {
            int portIdx = address.lastIndexOf(':');
            if (portIdx != -1 && address.lastIndexOf(']') < portIdx) {
                String portStr = address.substring(portIdx + 1);
                try {
                    int port = Integer.parseInt(portStr);
                    if (port < 1 || port > 65535) {
                        throw new InvalidArgumentException(
                            String.format(Locale.ENGLISH,
                                          "Invalid port number '%s' inside connection string (1:65535)", portStr)
                        );
                    }
                } catch (NumberFormatException ignore) {
                    throw new InvalidArgumentException(
                        String.format(Locale.ENGLISH,
                                      "Invalid port number '%s' inside connection string (1:65535)", portStr)
                    );
                }
                hosts.add(address);
            } else {
                hosts.add(address + ":" + DEFAULT_PORT);
            }
        }

        // parse the args part of the url
        var settingsBuilder = Settings.builder();
        String[] args = urlArgs.split("&");
        for (String token : args) {
            if (token.isEmpty()) {
                continue;
            }
            String settingName;
            String settingValue;
            int pos = token.indexOf('=');
            if (pos == -1) {
                settingName = token;
                settingValue = "";
            } else {
                settingName = token.substring(0, pos);
                settingValue = URLDecoder.decode(token.substring(pos + 1), StandardCharsets.UTF_8);
            }
            if (SUPPORTED_SETTINGS.contains(settingName) == false) {
                throw new InvalidArgumentException(
                    String.format(Locale.ENGLISH,
                                  "Connection string argument '%s' is not supported", settingName)
                );
            }
            settingsBuilder.put(settingName, settingValue);
        }

        return new ConnectionInfo(hosts, settingsBuilder.build());
    }

    private final List<String> hosts;
    private final Settings settings;

    public ConnectionInfo(List<String> hosts, Settings settings) {
        this.hosts = hosts;
        this.settings = settings;
    }

    public ConnectionInfo(StreamInput in) throws IOException {
        hosts = Arrays.stream(in.readStringArray()).toList();
        settings = Settings.readSettingsFromStream(in);
    }

    public List<String> hosts() {
        return hosts;
    }

    public Settings settings() {
        return settings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(hosts.toArray(new String[0]));
        Settings.writeSettingsToStream(settings, out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConnectionInfo that = (ConnectionInfo) o;
        return hosts.equals(that.hosts) && settings.equals(that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hosts, settings);
    }
}
