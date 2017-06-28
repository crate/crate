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


import io.crate.settings.CrateSetting;
import io.crate.settings.SharedSettings;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;

public class EnterpriseLoaderTest {

    private static CrateSetting<Boolean> mySetting = CrateSetting.of(
        Setting.boolSetting("testSetting", false, Setting.Property.NodeScope),
        DataTypes.BOOLEAN);

    private static final Settings allEnabled;
    private static final Settings onlyEnterprise;
    private static final Settings onlyOther;
    private static final Settings disabled;

    static {
        allEnabled = Settings.builder()
            .put(mySetting.setting().getKey(), true)
            .put(SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().getKey(), true)
            .build();
        onlyEnterprise = Settings.builder()
            .put(mySetting.setting().getKey(), false)
            .put(SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().getKey(), true)
            .build();
        onlyOther = Settings.builder()
            .put(mySetting.setting().getKey(), true)
            .put(SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().getKey(), false)
            .build();
        disabled = Settings.builder()
            .put(mySetting.setting().getKey(), false)
            .put(SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().getKey(), false)
            .build();
    }

    @Test
    public void testClassLoading() {
        Class<? extends TestClassBase> loadedClass =
            EnterpriseLoader.loadClass(allEnabled, mySetting, new TestClassLoadable());
        assertEquals("There should only be one class loaded at a time", TestClass.class, loadedClass);
    }

    @Test
    public void testClassInstantiation() {
        TestClassBase instantiatedClass =
            EnterpriseLoader.instantiateClass(allEnabled, mySetting, new TestClassLoadable());
        assertThat(instantiatedClass, instanceOf(TestClass.class));
    }

    @Test
    public void testClassInstantiationWithFallback() {
        TestClassBase instantiatedClass =
            EnterpriseLoader.instantiateClass(allEnabled, mySetting, new TestClassLoadableWithFallback());
        assertThat(instantiatedClass, instanceOf(TestClass.class));

        instantiatedClass =
            EnterpriseLoader.instantiateClass(onlyOther, mySetting, new TestClassLoadableWithFallback());
        assertThat(instantiatedClass, instanceOf(TestClassFallback.class));

        instantiatedClass =
            EnterpriseLoader.instantiateClass(disabled, mySetting, new TestClassLoadableWithFallback());
        assertThat(instantiatedClass, instanceOf(TestClassFallback.class));

        instantiatedClass =
            EnterpriseLoader.instantiateClass(onlyEnterprise, mySetting, new TestClassLoadableWithFallback());
        assertThat(instantiatedClass, instanceOf(TestClassFallback.class));
    }

    @Test
    public void testClassInstantiationWithParams() {
        TestClassBase instantiatedClass =
            EnterpriseLoader.instantiateClass(allEnabled, mySetting, new TestClassLoadableWithParams(), 42);
        assertThat(instantiatedClass, instanceOf(TestClass.class));
    }

    @Test
    public void testValueLoading() {
        String loadedValue = EnterpriseLoader.loadValue(allEnabled, mySetting, new TestValueLoadable());
        assertThat(loadedValue, is(TestClass.staticMethod()));

        loadedValue = EnterpriseLoader.loadValue(allEnabled, mySetting, new TestValueLoadableWithParams(), 42);
        assertThat(loadedValue, is(TestClass.staticMethodWithParams(42)));
    }

    @Test(expected = EnterpriseLoader.ClassLoadingException.class)
    public void testValueLoadingReturnType() {
        String loadedValue = EnterpriseLoader.loadValue(allEnabled, mySetting, new TestNonExistingValueLoadable());
        assertThat(loadedValue, is(TestClass.staticMethod()));
    }

    @Test(expected = EnterpriseLoader.ClassLoadingException.class)
    public void testValueInvalidType() {
        Long value =
            EnterpriseLoader.loadValue(allEnabled, mySetting, new TestWrongReturnType());
        assertThat(value, is(nullValue()));
    }

    @Test
    public void testClassReturnNull() {
        TestClassBase instantiatedClass;

        instantiatedClass =
            EnterpriseLoader.instantiateClass(disabled, mySetting, new WrongClassLoadableWithoutFallback());
        assertThat(instantiatedClass, is(nullValue()));

        instantiatedClass =
            EnterpriseLoader.instantiateClass(onlyEnterprise, mySetting, new WrongClassLoadableWithoutFallback());
        assertThat(instantiatedClass, is(nullValue()));

        instantiatedClass =
            EnterpriseLoader.instantiateClass(onlyOther, mySetting, new WrongClassLoadableWithoutFallback());
        assertThat(instantiatedClass, is(nullValue()));
    }


    private static class TestClassBase {}

    @SuppressWarnings("WeakerAccess")
    public static class TestClass extends TestClassBase {

        public TestClass() {}

        public TestClass(Integer integer) {}

        public static String staticMethod() {
            return "result";
        }

        public static String staticMethodWithParams(Integer integer) {
            return "result" + integer;
        }
    }

    private static class TestClassLoadable
        extends StringLoadable<TestClassBase>
        implements InstantiableClass<TestClassBase> {

        @Override
        public Class<TestClassBase> getBaseClass() {
            return TestClassBase.class;
        }

        @Override
        public String getFQClassName() {
            return "io.crate.enterprise.loader.EnterpriseLoaderTest$TestClass";
        }

        @Override
        public Class<?>[] getConstructorParams() {
            return new Class[] {};
        }
    }

    @SuppressWarnings("WeakerAccess")
    public static class TestClassFallback extends TestClassBase {}

    private static class TestClassLoadableWithFallback
        extends TestClassLoadable
        implements InstantiableClass.WithFallback<TestClassBase> {

        @Override
        public Class<? extends TestClassBase> getFallback() {
            return TestClassFallback.class;
        }
    }

    private static class WrongClassLoadableWithoutFallback extends TestClassLoadable {

        @Override
        public String getFQClassName() {
            return "wrongName";
        }
    }

    private static class TestValueLoadable
        extends TestClassLoadable
        implements LoadableValue<String, TestClassBase> {

        @Override
        public String methodName() {
            return "staticMethod";
        }

        @Override
        public Class<?>[] getMethodParams() {
            return new Class[] {};
        }

        @Override
        public Class<String> getReturnType() {
            return String.class;
        }
    }

    private static class TestValueLoadableWithParams extends TestValueLoadable {

        @Override
        public String methodName() {
            return "staticMethodWithParams";
        }

        @Override
        public Class<?>[] getMethodParams() {
            return new Class[] {Integer.class};
        }
    }

    private static class TestNonExistingValueLoadable
            extends TestValueLoadable{

        @Override
        public String methodName() {
            return "wrongmethodname";
        }
    }


    private static class TestWrongReturnType
        extends TestClassLoadable
        implements LoadableValue<Long, TestClassBase> {

        @Override
        public String methodName() {
            return "staticMethod";
        }

        @Override
        public Class<?>[] getMethodParams() {
            return new Class[] {};
        }

        @Override
        public Class<Long> getReturnType() {
            return Long.class;
        }
    }

    private static class TestClassLoadableWithParams extends TestClassLoadable {

        @Override
        public Class<?>[] getConstructorParams() {
            return new Class[] { Integer.class };
        }
    }

}
