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

package io.crate.protocols.postgres;

import io.crate.protocols.ssl.SslConfigSettings;
import io.crate.protocols.ssl.SslConfigurationException;
import io.crate.settings.SharedSettings;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.lang.reflect.InvocationTargetException;

/**
 * Loads the appropriate implementation of the SslReqHandler.
 */
public class SslReqHandlerLoader {

    private static final Logger LOGGER = Loggers.getLogger(SslReqHandlerLoader.class);
    private static final String SSL_IMPL_CLASS = "io.crate.protocols.postgres.SslReqConfiguringHandler";

    private SslReqHandlerLoader() {}

    /**
     * Loads the SslRequest handler. Should only be called once per application life time.
     * @throws SslConfigurationException Exception thrown if the user has supplied an invalid configuration
     */
    public static SslReqHandler load(Settings settings) {
        boolean enterpriseEnabled = SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().get(settings);
        boolean sslEnabled = SslConfigSettings.SSL_ENABLED.setting().get(settings);
        if (enterpriseEnabled && sslEnabled) {
            ClassLoader classLoader = ClassLoader.getSystemClassLoader();
            try {
                return classLoader
                    .loadClass(SSL_IMPL_CLASS)
                    .asSubclass(SslReqHandler.class)
                    .getDeclaredConstructor(Settings.class)
                    .newInstance(settings);
            } catch (Throwable e) {
                // The JVM wraps the exception of dynamically loaded classes into an InvocationTargetException
                // which we need to unpack first to see if we have an SslConfigurationException.
                tryUnwrapSslConfigurationException(e);
                throw new SslHandlerLoadingException(e);
            }
        }
        return new SslReqRejectingHandler(settings);
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
