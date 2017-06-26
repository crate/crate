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

package io.crate.protocols.ssl;

import io.crate.protocols.http.DefaultHttpsHandler;
import io.crate.protocols.http.HttpsHandler;
import io.crate.protocols.postgres.SslReqHandler;
import io.crate.protocols.postgres.SslReqRejectingHandler;
import io.crate.settings.CrateSetting;
import io.crate.settings.SharedSettings;
import io.netty.handler.ssl.SslContext;
import org.elasticsearch.common.settings.Settings;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

/**
 * Loads the appropriate implementation of the PSQL SSL and Https handlers.
 */
public class SslHandlerLoader {

    private static final String PSQL_HANDLER_CLAZZ = "io.crate.protocols.postgres.SslReqConfiguringHandler";
    private static final String HTTPS_HANDLER_CLAZZ = "io.crate.protocols.http.HttpsConfiguringHandler";

    private SslHandlerLoader() {}

    /**
     * Loads the SslRequest handler. Should only be called once per application life time.
     * @throws SslConfigurationException Exception thrown if the user has supplied an invalid configuration
     */
    public static SslReqHandler loadSslReqHandler(Settings settings) {
        return load(settings,
            SslConfigSettings.SSL_PSQL_ENABLED,
            PSQL_HANDLER_CLAZZ,
            SslReqHandler.class,
            () -> new SslReqRejectingHandler(settings));
    }

    public static HttpsHandler loadHttpsHandler(Settings settings) {
        return load(settings,
            SslConfigSettings.SSL_HTTP_ENABLED,
            HTTPS_HANDLER_CLAZZ,
            HttpsHandler.class,
            () -> new DefaultHttpsHandler(settings));
    }

    private static <T> T load(Settings settings,
                              CrateSetting<Boolean> sslSetting,
                              String clazz,
                              Class<T> baseClazz,
                              Supplier<T> fallback) {
        boolean enterpriseEnabled = SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().get(settings);
        boolean sslEnabled = sslSetting.setting().get(settings);
        if (enterpriseEnabled && sslEnabled) {
            ClassLoader classLoader = ClassLoader.getSystemClassLoader();
            try {
                return classLoader
                    .loadClass(clazz)
                    .asSubclass(baseClazz)
                    .getDeclaredConstructor(Settings.class)
                    .newInstance(settings);
            } catch (Throwable e) {
                // The JVM wraps the exception of dynamically loaded classes into an InvocationTargetException
                // which we need to unpack first to see if we have an SslConfigurationException.
                tryUnwrapSslConfigurationException(e);
                throw new SslHandlerLoadingException(e);
            }
        }
        return fallback.get();
    }

    private static void tryUnwrapSslConfigurationException(Throwable e) {
        if (e instanceof InvocationTargetException) {
            Throwable cause = e.getCause();
            if (cause instanceof SslConfigurationException) {
                throw (SslConfigurationException) cause;
            }
        }
    }

    private static class SslHandlerLoadingException extends RuntimeException {
        SslHandlerLoadingException(Throwable cause) {
            super("Loading the SslConfiguringHandler failed although enterprise is enabled.", cause);
        }
    }
}
