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

package io.crate.enterprise.loader;

import com.google.common.annotations.VisibleForTesting;
import io.crate.settings.CrateSetting;
import io.crate.settings.SharedSettings;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.settings.Settings;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Loads classes or values which are only available at runtime.
 */
public class EnterpriseLoader {

    private EnterpriseLoader() {}

    @VisibleForTesting
    static <T> Class<? extends T> loadClass(Settings settings,
                                            @Nullable CrateSetting<Boolean> enabledSetting,
                                            LoadableClass<T> loadableClass) {
        boolean enterpriseEnabled = SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().get(settings);
        boolean settingEnabled = enabledSetting == null || enabledSetting.setting().get(settings);
        if (enterpriseEnabled && settingEnabled) {
            try {
                return loadableClass.loadClass();
            } catch (ClassNotFoundException ignored) {
                return null;
            } catch (Throwable t) {
                // The JVM wraps the exception of dynamically loaded classes into an InvocationTargetException
                // which we need to unpack first to see if we have an SslConfigurationException.
                tryUnwrapInvocationException(t);
                throw new ClassLoadingException(
                    "Loading the enterprise class failed although enterprise is enabled.", t);
            }
        }
        return null;
    }

    public static <T> T instantiateClass(Settings settings,
                                         @Nullable CrateSetting<Boolean> enabledSetting,
                                         InstantiableClass<T> instantiableClass,
                                         Object... args) {
        final Class<? extends T> loadedClass = loadClass(settings, enabledSetting, instantiableClass);
        final T instance;
        if (loadedClass == null) {
            if (!(instantiableClass instanceof InstantiableClass.WithFallback)) {
                return null;
            }
            InstantiableClass.WithFallback<T> icwf = (InstantiableClass.WithFallback<T>) instantiableClass;
            try {
                Class<? extends T> fallbackClazz = icwf.getFallback();
                instance = fallbackClazz
                    .getDeclaredConstructor(icwf.getConstructorParams())
                    .newInstance(args);
                return instance;
            } catch (Throwable t) {
                throw new ClassLoadingException("Loading the fallback failed", t);
            }
        } else {
            try {
                instance = loadedClass
                    .getDeclaredConstructor(instantiableClass.getConstructorParams())
                    .newInstance(args);
                return instance;
            } catch (Throwable t) {
                // The JVM wraps the exception of dynamically loaded classes into an InvocationTargetException
                // which we need to unpack first to see if we have an SslConfigurationException.
                tryUnwrapInvocationException(t);
                throw new ClassLoadingException(
                    "Instantiating the enterprise class failed although enterprise is enabled.", t);
            }
        }
    }

    public static <V, T> V loadValue(Settings settings,
                                     @Nullable CrateSetting<Boolean> enabledSetting,
                                     LoadableValue<V, T> loadableValue,
                                     Object... args) {
        Class<? extends T> loadedClass = loadClass(settings, enabledSetting, loadableValue);
        if (loadedClass == null) {
            return null;
        }
        String methodName = loadableValue.methodName();
        final Object value;
        try {
            Method method = loadedClass.getMethod(methodName, loadableValue.getMethodParams());
            value = method.invoke(null, args);
        } catch (Throwable t) {
            tryUnwrapInvocationException(t);
            throw new ClassLoadingException("Couldn't load the value", t);
        }

        Class returnClassType = loadableValue.getReturnType();
        if (!value.getClass().isAssignableFrom(returnClassType)) {
            throw new ClassLoadingException("Return type for loaded value was not correct.");
        }

        //noinspection unchecked
        return (V) value;
    }

    private static void tryUnwrapInvocationException(Throwable e) {
        if (e instanceof InvocationTargetException) {
            Throwable cause = e.getCause();
            throw new ClassLoadingException("Failed to invoke the loaded class", cause);
        }
    }

    static class ClassLoadingException extends RuntimeException {

        ClassLoadingException(String msg, Throwable cause) {
            super(msg, cause);
        }

        private ClassLoadingException(String message) {
            super(message);
        }
    }
}
