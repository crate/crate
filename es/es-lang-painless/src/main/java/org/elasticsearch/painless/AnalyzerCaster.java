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

package org.elasticsearch.painless;

import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;

import java.util.Objects;

/**
 * Used during the analysis phase to collect legal type casts and promotions
 * for type-checking and later to write necessary casts in the bytecode.
 */
public final class AnalyzerCaster {

    public static PainlessCast getLegalCast(Location location, Class<?> actual, Class<?> expected, boolean explicit, boolean internal) {
        Objects.requireNonNull(actual);
        Objects.requireNonNull(expected);

        if (actual == expected) {
            return null;
        }

        if (actual == def.class) {
            if (expected == boolean.class) {
                return PainlessCast.unboxTargetType(def.class, Boolean.class, explicit, boolean.class);
            } else if (expected == byte.class) {
                return PainlessCast.unboxTargetType(def.class, Byte.class, explicit, byte.class);
            } else if (expected == short.class) {
                return PainlessCast.unboxTargetType(def.class, Short.class, explicit, short.class);
            } else if (expected == char.class) {
                return PainlessCast.unboxTargetType(def.class, Character.class, explicit, char.class);
            } else if (expected == int.class) {
                return PainlessCast.unboxTargetType(def.class, Integer.class, explicit, int.class);
            } else if (expected == long.class) {
                return PainlessCast.unboxTargetType(def.class, Long.class, explicit, long.class);
            } else if (expected == float.class) {
                return PainlessCast.unboxTargetType(def.class, Float.class, explicit, float.class);
            } else if (expected == double.class) {
                return PainlessCast.unboxTargetType(def.class, Double.class, explicit, double.class);
            }
        } else if (actual == Object.class) {
            if (expected == byte.class && explicit && internal) {
                return PainlessCast.unboxTargetType(Object.class, Byte.class, true, byte.class);
            } else if (expected == short.class && explicit && internal) {
                return PainlessCast.unboxTargetType(Object.class, Short.class, true, short.class);
            } else if (expected == char.class && explicit && internal) {
                return PainlessCast.unboxTargetType(Object.class, Character.class, true, char.class);
            } else if (expected == int.class && explicit && internal) {
                return PainlessCast.unboxTargetType(Object.class, Integer.class, true, int.class);
            } else if (expected == long.class && explicit && internal) {
                return PainlessCast.unboxTargetType(Object.class, Long.class, true, long.class);
            } else if (expected == float.class && explicit && internal) {
                return PainlessCast.unboxTargetType(Object.class, Float.class, true, float.class);
            } else if (expected == double.class && explicit && internal) {
                return PainlessCast.unboxTargetType(Object.class, Double.class, true, double.class);
            }
        } else if (actual == Number.class) {
            if (expected == byte.class && explicit && internal) {
                return PainlessCast.unboxTargetType(Number.class, Byte.class, true, byte.class);
            } else if (expected == short.class && explicit && internal) {
                return PainlessCast.unboxTargetType(Number.class, Short.class, true, short.class);
            } else if (expected == char.class && explicit && internal) {
                return PainlessCast.unboxTargetType(Number.class, Character.class, true, char.class);
            } else if (expected == int.class && explicit && internal) {
                return PainlessCast.unboxTargetType(Number.class, Integer.class, true, int.class);
            } else if (expected == long.class && explicit && internal) {
                return PainlessCast.unboxTargetType(Number.class, Long.class, true, long.class);
            } else if (expected == float.class && explicit && internal) {
                return PainlessCast.unboxTargetType(Number.class, Float.class, true, float.class);
            } else if (expected == double.class && explicit && internal) {
                return PainlessCast.unboxTargetType(Number.class, Double.class, true, double.class);
            }
        } else if (actual == String.class) {
            if (expected == char.class && explicit) {
                return PainlessCast.originalTypetoTargetType(String.class, char.class, true);
            }
        } else if (actual == boolean.class) {
            if (expected == def.class) {
                return PainlessCast.boxOriginalType(Boolean.class, def.class, explicit, boolean.class);
            } else if (expected == Object.class && internal) {
                return PainlessCast.boxOriginalType(Boolean.class, Object.class, explicit, boolean.class);
            } else if (expected == Boolean.class && internal) {
                return PainlessCast.boxTargetType(boolean.class, boolean.class, explicit, boolean.class);
            }
        } else if (actual == byte.class) {
            if (expected == def.class) {
                return PainlessCast.boxOriginalType(Byte.class, def.class, explicit, byte.class);
            } else if (expected == Object.class && internal) {
                return PainlessCast.boxOriginalType(Byte.class, Object.class, explicit, byte.class);
            } else if (expected == Number.class && internal) {
                return PainlessCast.boxOriginalType(Byte.class, Number.class, explicit, byte.class);
            } else if (expected == short.class) {
                return PainlessCast.originalTypetoTargetType(byte.class, short.class, explicit);
            } else if (expected == char.class && explicit) {
                return PainlessCast.originalTypetoTargetType(byte.class, char.class, true);
            } else if (expected == int.class) {
                return PainlessCast.originalTypetoTargetType(byte.class, int.class, explicit);
            } else if (expected == long.class) {
                return PainlessCast.originalTypetoTargetType(byte.class, long.class, explicit);
            } else if (expected == float.class) {
                return PainlessCast.originalTypetoTargetType(byte.class, float.class, explicit);
            } else if (expected == double.class) {
                return PainlessCast.originalTypetoTargetType(byte.class, double.class, explicit);
            } else if (expected == Byte.class && internal) {
                return PainlessCast.boxTargetType(byte.class, byte.class, explicit, byte.class);
            } else if (expected == Short.class && internal) {
                return PainlessCast.boxTargetType(byte.class, short.class, explicit, short.class);
            } else if (expected == Character.class && explicit && internal) {
                return PainlessCast.boxTargetType(byte.class, char.class, true, char.class);
            } else if (expected == Integer.class && internal) {
                return PainlessCast.boxTargetType(byte.class, int.class, explicit, int.class);
            } else if (expected == Long.class && internal) {
                return PainlessCast.boxTargetType(byte.class, long.class, explicit, long.class);
            } else if (expected == Float.class && internal) {
                return PainlessCast.boxTargetType(byte.class, float.class, explicit, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.boxTargetType(byte.class, double.class, explicit, double.class);
            }
        } else if (actual == short.class) {
            if (expected == def.class) {
                return PainlessCast.boxOriginalType(Short.class, def.class, explicit, short.class);
            } else if (expected == Object.class && internal) {
                return PainlessCast.boxOriginalType(Short.class, Object.class, explicit, short.class);
            } else if (expected == Number.class && internal) {
                return PainlessCast.boxOriginalType(Short.class, Number.class, explicit, short.class);
            } else if (expected == byte.class && explicit) {
                return PainlessCast.originalTypetoTargetType(short.class, byte.class, true);
            } else if (expected == char.class && explicit) {
                return PainlessCast.originalTypetoTargetType(short.class, char.class, true);
            } else if (expected == int.class) {
                return PainlessCast.originalTypetoTargetType(short.class, int.class, explicit);
            } else if (expected == long.class) {
                return PainlessCast.originalTypetoTargetType(short.class, long.class, explicit);
            } else if (expected == float.class) {
                return PainlessCast.originalTypetoTargetType(short.class, float.class, explicit);
            } else if (expected == double.class) {
                return PainlessCast.originalTypetoTargetType(short.class, double.class, explicit);
            } else if (expected == Byte.class && explicit && internal) {
                return PainlessCast.boxTargetType(short.class, byte.class, true, byte.class);
            } else if (expected == Short.class && internal) {
                return PainlessCast.boxTargetType(short.class, short.class, explicit, short.class);
            } else if (expected == Character.class && explicit && internal) {
                return PainlessCast.boxTargetType(short.class, char.class, true, char.class);
            } else if (expected == Integer.class && internal) {
                return PainlessCast.boxTargetType(short.class, int.class, explicit, int.class);
            } else if (expected == Long.class && internal) {
                return PainlessCast.boxTargetType(short.class, long.class, explicit, long.class);
            } else if (expected == Float.class && internal) {
                return PainlessCast.boxTargetType(short.class, float.class, explicit, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.boxTargetType(short.class, double.class, explicit, double.class);
            }
        } else if (actual == char.class) {
            if (expected == def.class) {
                return PainlessCast.boxOriginalType(Character.class, def.class, explicit, char.class);
            } else if (expected == Object.class && internal) {
                return PainlessCast.boxOriginalType(Character.class, Object.class, explicit, char.class);
            } else if (expected == Number.class && internal) {
                return PainlessCast.boxOriginalType(Character.class, Number.class, explicit, char.class);
            } else if (expected == String.class) {
                return PainlessCast.originalTypetoTargetType(char.class, String.class, explicit);
            } else if (expected == byte.class && explicit) {
                return PainlessCast.originalTypetoTargetType(char.class, byte.class, true);
            } else if (expected == short.class && explicit) {
                return PainlessCast.originalTypetoTargetType(char.class, short.class, true);
            } else if (expected == int.class) {
                return PainlessCast.originalTypetoTargetType(char.class, int.class, explicit);
            } else if (expected == long.class) {
                return PainlessCast.originalTypetoTargetType(char.class, long.class, explicit);
            } else if (expected == float.class) {
                return PainlessCast.originalTypetoTargetType(char.class, float.class, explicit);
            } else if (expected == double.class) {
                return PainlessCast.originalTypetoTargetType(char.class, double.class, explicit);
            } else if (expected == Byte.class && explicit && internal) {
                return PainlessCast.boxTargetType(char.class, byte.class, true, byte.class);
            } else if (expected == Short.class && internal) {
                return PainlessCast.boxTargetType(char.class, short.class, explicit, short.class);
            } else if (expected == Character.class && internal) {
                return PainlessCast.boxTargetType(char.class, char.class, true, char.class);
            } else if (expected == Integer.class && internal) {
                return PainlessCast.boxTargetType(char.class, int.class, explicit, int.class);
            } else if (expected == Long.class && internal) {
                return PainlessCast.boxTargetType(char.class, long.class, explicit, long.class);
            } else if (expected == Float.class && internal) {
                return PainlessCast.boxTargetType(char.class, float.class, explicit, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.boxTargetType(char.class, double.class, explicit, double.class);
            }
        } else if (actual == int.class) {
            if (expected == def.class) {
                return PainlessCast.boxOriginalType(Integer.class, def.class, explicit, int.class);
            } else if (expected == Object.class && internal) {
                return PainlessCast.boxOriginalType(Integer.class, Object.class, explicit, int.class);
            } else if (expected == Number.class && internal) {
                return PainlessCast.boxOriginalType(Integer.class, Number.class, explicit, int.class);
            } else if (expected == byte.class && explicit) {
                return PainlessCast.originalTypetoTargetType(int.class, byte.class, true);
            } else if (expected == char.class && explicit) {
                return PainlessCast.originalTypetoTargetType(int.class, char.class, true);
            } else if (expected == short.class && explicit) {
                return PainlessCast.originalTypetoTargetType(int.class, short.class, true);
            } else if (expected == long.class) {
                return PainlessCast.originalTypetoTargetType(int.class, long.class, explicit);
            } else if (expected == float.class) {
                return PainlessCast.originalTypetoTargetType(int.class, float.class, explicit);
            } else if (expected == double.class) {
                return PainlessCast.originalTypetoTargetType(int.class, double.class, explicit);
            } else if (expected == Byte.class && explicit && internal) {
                return PainlessCast.boxTargetType(int.class, byte.class, true, byte.class);
            } else if (expected == Short.class && explicit && internal) {
                return PainlessCast.boxTargetType(int.class, short.class, true, short.class);
            } else if (expected == Character.class && explicit && internal) {
                return PainlessCast.boxTargetType(int.class, char.class, true, char.class);
            } else if (expected == Integer.class && internal) {
                return PainlessCast.boxTargetType(int.class, int.class, explicit, int.class);
            } else if (expected == Long.class && internal) {
                return PainlessCast.boxTargetType(int.class, long.class, explicit, long.class);
            } else if (expected == Float.class && internal) {
                return PainlessCast.boxTargetType(int.class, float.class, explicit, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.boxTargetType(int.class, double.class, explicit, double.class);
            }
        } else if (actual == long.class) {
            if (expected == def.class) {
                return PainlessCast.boxOriginalType(Long.class, def.class, explicit, long.class);
            } else if (expected == Object.class && internal) {
                return PainlessCast.boxOriginalType(Long.class, Object.class, explicit, long.class);
            } else if (expected == Number.class && internal) {
                return PainlessCast.boxOriginalType(Long.class, Number.class, explicit, long.class);
            } else if (expected == byte.class && explicit) {
                return PainlessCast.originalTypetoTargetType(long.class, byte.class, true);
            } else if (expected == char.class && explicit) {
                return PainlessCast.originalTypetoTargetType(long.class, char.class, true);
            } else if (expected == short.class && explicit) {
                return PainlessCast.originalTypetoTargetType(long.class, short.class, true);
            } else if (expected == int.class && explicit) {
                return PainlessCast.originalTypetoTargetType(long.class, int.class, true);
            } else if (expected == float.class) {
                return PainlessCast.originalTypetoTargetType(long.class, float.class, explicit);
            } else if (expected == double.class) {
                return PainlessCast.originalTypetoTargetType(long.class, double.class, explicit);
            } else if (expected == Byte.class && explicit && internal) {
                return PainlessCast.boxTargetType(long.class, byte.class, true, byte.class);
            } else if (expected == Short.class && explicit && internal) {
                return PainlessCast.boxTargetType(long.class, short.class, true, short.class);
            } else if (expected == Character.class && explicit && internal) {
                return PainlessCast.boxTargetType(long.class, char.class, true, char.class);
            } else if (expected == Integer.class && explicit && internal) {
                return PainlessCast.boxTargetType(long.class, int.class, true, int.class);
            } else if (expected == Long.class && internal) {
                return PainlessCast.boxTargetType(long.class, long.class, explicit, long.class);
            } else if (expected == Float.class && internal) {
                return PainlessCast.boxTargetType(long.class, float.class, explicit, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.boxTargetType(long.class, double.class, explicit, double.class);
            }
        } else if (actual == float.class) {
            if (expected == def.class) {
                return PainlessCast.boxOriginalType(Float.class, def.class, explicit, float.class);
            } else if (expected == Object.class && internal) {
                return PainlessCast.boxOriginalType(Float.class, Object.class, explicit, float.class);
            } else if (expected == Number.class && internal) {
                return PainlessCast.boxOriginalType(Float.class, Number.class, explicit, float.class);
            } else if (expected == byte.class && explicit) {
                return PainlessCast.originalTypetoTargetType(float.class, byte.class, true);
            } else if (expected == char.class && explicit) {
                return PainlessCast.originalTypetoTargetType(float.class, char.class, true);
            } else if (expected == short.class && explicit) {
                return PainlessCast.originalTypetoTargetType(float.class, short.class, true);
            } else if (expected == int.class && explicit) {
                return PainlessCast.originalTypetoTargetType(float.class, int.class, true);
            } else if (expected == long.class && explicit) {
                return PainlessCast.originalTypetoTargetType(float.class, long.class, true);
            } else if (expected == double.class) {
                return PainlessCast.originalTypetoTargetType(float.class, double.class, explicit);
            } else if (expected == Byte.class && explicit && internal) {
                return PainlessCast.boxTargetType(float.class, byte.class, true, byte.class);
            } else if (expected == Short.class && explicit && internal) {
                return PainlessCast.boxTargetType(float.class, short.class, true, short.class);
            } else if (expected == Character.class && explicit && internal) {
                return PainlessCast.boxTargetType(float.class, char.class, true, char.class);
            } else if (expected == Integer.class && explicit && internal) {
                return PainlessCast.boxTargetType(float.class, int.class, true, int.class);
            } else if (expected == Long.class && explicit && internal) {
                return PainlessCast.boxTargetType(float.class, long.class, true, long.class);
            } else if (expected == Float.class && internal) {
                return PainlessCast.boxTargetType(float.class, float.class, explicit, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.boxTargetType(float.class, double.class, explicit, double.class);
            }
        } else if (actual == double.class) {
            if (expected == def.class) {
                return PainlessCast.boxOriginalType(Double.class, def.class, explicit, double.class);
            } else if (expected == Object.class && internal) {
                return PainlessCast.boxOriginalType(Double.class, Object.class, explicit, double.class);
            } else if (expected == Number.class && internal) {
                return PainlessCast.boxOriginalType(Double.class, Number.class, explicit, double.class);
            } else if (expected == byte.class && explicit) {
                return PainlessCast.originalTypetoTargetType(double.class, byte.class, true);
            } else if (expected == char.class && explicit) {
                return PainlessCast.originalTypetoTargetType(double.class, char.class, true);
            } else if (expected == short.class && explicit) {
                return PainlessCast.originalTypetoTargetType(double.class, short.class, true);
            } else if (expected == int.class && explicit) {
                return PainlessCast.originalTypetoTargetType(double.class, int.class, true);
            } else if (expected == long.class && explicit) {
                return PainlessCast.originalTypetoTargetType(double.class, long.class, true);
            } else if (expected == float.class && explicit) {
                return PainlessCast.originalTypetoTargetType(double.class, float.class, true);
            } else if (expected == Byte.class && explicit && internal) {
                return PainlessCast.boxTargetType(double.class, byte.class, true, byte.class);
            } else if (expected == Short.class && explicit && internal) {
                return PainlessCast.boxTargetType(double.class, short.class, true, short.class);
            } else if (expected == Character.class && explicit && internal) {
                return PainlessCast.boxTargetType(double.class, char.class, true, char.class);
            } else if (expected == Integer.class && explicit && internal) {
                return PainlessCast.boxTargetType(double.class, int.class, true, int.class);
            } else if (expected == Long.class && explicit && internal) {
                return PainlessCast.boxTargetType(double.class, long.class, true, long.class);
            } else if (expected == Float.class && explicit && internal) {
                return PainlessCast.boxTargetType(double.class, float.class, true, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.boxTargetType(double.class, double.class, explicit, double.class);
            }
        } else if (actual == Boolean.class) {
            if (expected == boolean.class && internal) {
                return PainlessCast.unboxOriginalType(boolean.class, boolean.class, explicit, boolean.class);
            }
        } else if (actual == Byte.class) {
            if (expected == byte.class && internal) {
                return PainlessCast.unboxOriginalType(byte.class, byte.class, explicit, byte.class);
            } else if (expected == short.class && internal) {
                return PainlessCast.unboxOriginalType(byte.class, short.class, explicit, byte.class);
            } else if (expected == char.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(byte.class, char.class, true, byte.class);
            } else if (expected == int.class && internal) {
                return PainlessCast.unboxOriginalType(byte.class, int.class, explicit, byte.class);
            } else if (expected == long.class && internal) {
                return PainlessCast.unboxOriginalType(byte.class, long.class, explicit, byte.class);
            } else if (expected == float.class && internal) {
                return PainlessCast.unboxOriginalType(byte.class, float.class, explicit, byte.class);
            } else if (expected == double.class && internal) {
                return PainlessCast.unboxOriginalType(byte.class, double.class, explicit, byte.class);
            }
        } else if (actual == Short.class) {
            if (expected == byte.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(short.class, byte.class, true, short.class);
            } else if (expected == short.class && internal) {
                return PainlessCast.unboxOriginalType(short.class, short.class, explicit, short.class);
            } else if (expected == char.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(short.class, char.class, true, short.class);
            } else if (expected == int.class && internal) {
                return PainlessCast.unboxOriginalType(short.class, int.class, explicit, short.class);
            } else if (expected == long.class && internal) {
                return PainlessCast.unboxOriginalType(short.class, long.class, explicit, short.class);
            } else if (expected == float.class && internal) {
                return PainlessCast.unboxOriginalType(short.class, float.class, explicit, short.class);
            } else if (expected == double.class && internal) {
                return PainlessCast.unboxOriginalType(short.class, double.class, explicit, short.class);
            }
        } else if (actual == Character.class) {
            if (expected == byte.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(char.class, byte.class, true, char.class);
            } else if (expected == short.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(char.class, short.class, true, char.class);
            } else if (expected == char.class && internal) {
                return PainlessCast.unboxOriginalType(char.class, char.class, explicit, char.class);
            } else if (expected == int.class && internal) {
                return PainlessCast.unboxOriginalType(char.class, int.class, explicit, char.class);
            } else if (expected == long.class && internal) {
                return PainlessCast.unboxOriginalType(char.class, long.class, explicit, char.class);
            } else if (expected == float.class && internal) {
                return PainlessCast.unboxOriginalType(char.class, float.class, explicit, char.class);
            } else if (expected == double.class && internal) {
                return PainlessCast.unboxOriginalType(char.class, double.class, explicit, char.class);
            }
        } else if (actual == Integer.class) {
            if (expected == byte.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(int.class, byte.class, true, int.class);
            } else if (expected == short.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(int.class, short.class, true, int.class);
            } else if (expected == char.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(int.class, char.class, true, int.class);
            } else if (expected == int.class && internal) {
                return PainlessCast.unboxOriginalType(int.class, int.class, explicit, int.class);
            } else if (expected == long.class && internal) {
                return PainlessCast.unboxOriginalType(int.class, long.class, explicit, int.class);
            } else if (expected == float.class && internal) {
                return PainlessCast.unboxOriginalType(int.class, float.class, explicit, int.class);
            } else if (expected == double.class && internal) {
                return PainlessCast.unboxOriginalType(int.class, double.class, explicit, int.class);
            }
        } else if (actual == Long.class) {
            if (expected == byte.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(long.class, byte.class, true, long.class);
            } else if (expected == short.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(long.class, short.class, true, long.class);
            } else if (expected == char.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(long.class, char.class, true, long.class);
            } else if (expected == int.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(long.class, int.class, true, long.class);
            } else if (expected == long.class && internal) {
                return PainlessCast.unboxOriginalType(long.class, long.class, explicit, long.class);
            } else if (expected == float.class && internal) {
                return PainlessCast.unboxOriginalType(long.class, float.class, explicit, long.class);
            } else if (expected == double.class && internal) {
                return PainlessCast.unboxOriginalType(long.class, double.class, explicit, long.class);
            }
        } else if (actual == Float.class) {
            if (expected == byte.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(float.class, byte.class, true, float.class);
            } else if (expected == short.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(float.class, short.class, true, float.class);
            } else if (expected == char.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(float.class, char.class, true, float.class);
            } else if (expected == int.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(float.class, int.class, true, float.class);
            } else if (expected == long.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(float.class, long.class, true, float.class);
            } else if (expected == float.class && internal) {
                return PainlessCast.unboxOriginalType(float.class, float.class, explicit, float.class);
            } else if (expected == double.class && internal) {
                return PainlessCast.unboxOriginalType(float.class, double.class, explicit, float.class);
            }
        } else if (actual == Double.class) {
            if (expected == byte.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(double.class, byte.class, true, double.class);
            } else if (expected == short.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(double.class, short.class, true, double.class);
            } else if (expected == char.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(double.class, char.class, true, double.class);
            } else if (expected == int.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(double.class, int.class, true, double.class);
            } else if (expected == long.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(double.class, long.class, true, double.class);
            } else if (expected == float.class && explicit && internal) {
                return PainlessCast.unboxOriginalType(double.class, float.class, true, double.class);
            } else if (expected == double.class && internal) {
                return PainlessCast.unboxOriginalType(double.class, double.class, explicit, double.class);
            }
        }

        if (    actual == def.class                                   ||
                (actual != void.class && expected == def.class) ||
                expected.isAssignableFrom(actual)    ||
                (actual.isAssignableFrom(expected) && explicit)) {
            return PainlessCast.originalTypetoTargetType(actual, expected, explicit);
        } else {
            throw location.createError(new ClassCastException("Cannot cast from " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(actual) + "] to " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(expected) + "]."));
        }
    }

    public static Object constCast(Location location, Object constant, PainlessCast cast) {
        Class<?> fsort = cast.originalType;
        Class<?> tsort = cast.targetType;

        if (fsort == tsort) {
            return constant;
        } else if (fsort == String.class && tsort == char.class) {
            return Utility.StringTochar((String)constant);
        } else if (fsort == char.class && tsort == String.class) {
            return Utility.charToString((char)constant);
        } else if (fsort.isPrimitive() && fsort != boolean.class && tsort.isPrimitive() && tsort != boolean.class) {
            Number number;

            if (fsort == char.class) {
                number = (int)(char)constant;
            } else {
                number = (Number)constant;
            }

            if      (tsort == byte.class) return number.byteValue();
            else if (tsort == short.class) return number.shortValue();
            else if (tsort == char.class) return (char)number.intValue();
            else if (tsort == int.class) return number.intValue();
            else if (tsort == long.class) return number.longValue();
            else if (tsort == float.class) return number.floatValue();
            else if (tsort == double.class) return number.doubleValue();
            else {
                throw location.createError(new IllegalStateException("Cannot cast from " +
                    "[" + cast.originalType.getCanonicalName() + "] to [" + cast.targetType.getCanonicalName() + "]."));
            }
        } else {
            throw location.createError(new IllegalStateException("Cannot cast from " +
                "[" + cast.originalType.getCanonicalName() + "] to [" + cast.targetType.getCanonicalName() + "]."));
        }
    }

    public static Class<?> promoteNumeric(Class<?> from, boolean decimal) {
        if (from == def.class || from == double.class && decimal || from == float.class && decimal || from == long.class) {
            return from;
        } else if (from == int.class || from == char.class || from == short.class || from == byte.class) {
            return int.class;
        }

        return null;
    }

    public static Class<?> promoteNumeric(Class<?> from0, Class<?> from1, boolean decimal) {
        if (from0 == def.class || from1 == def.class) {
            return def.class;
        }

        if (decimal) {
            if (from0 == double.class || from1 == double.class) {
                return double.class;
            } else if (from0 == float.class || from1 == float.class) {
                return float.class;
            }
        }

        if (from0 == long.class || from1 == long.class) {
            return long.class;
        } else if (from0 == int.class   || from1 == int.class   ||
                   from0 == char.class  || from1 == char.class  ||
                   from0 == short.class || from1 == short.class ||
                   from0 == byte.class  || from1 == byte.class) {
            return int.class;
        }

        return null;
    }

    public static Class<?> promoteAdd(Class<?> from0, Class<?> from1) {
        if (from0 == String.class || from1 == String.class) {
            return String.class;
        }

        return promoteNumeric(from0, from1, true);
    }

    public static Class<?> promoteXor(Class<?> from0, Class<?> from1) {
        if (from0 == def.class || from1 == def.class) {
            return def.class;
        }

        if (from0 == boolean.class || from1 == boolean.class) {
            return boolean.class;
        }

        return promoteNumeric(from0, from1, false);
    }

    public static Class<?> promoteEquality(Class<?> from0, Class<?> from1) {
        if (from0 == def.class || from1 == def.class) {
            return def.class;
        }

        if (from0.isPrimitive() && from1.isPrimitive()) {
            if (from0 == boolean.class && from1 == boolean.class) {
                return boolean.class;
            }

            return promoteNumeric(from0, from1, true);
        }

        return Object.class;
    }

    public static Class<?> promoteConditional(Class<?> from0, Class<?> from1, Object const0, Object const1) {
        if (from0 == from1) {
            return from0;
        }

        if (from0 == def.class || from1 == def.class) {
            return def.class;
        }

        if (from0.isPrimitive() && from1.isPrimitive()) {
            if (from0 == boolean.class && from1 == boolean.class) {
                return boolean.class;
            }

            if (from0 == double.class || from1 == double.class) {
                return double.class;
            } else if (from0 == float.class || from1 == float.class) {
                return float.class;
            } else if (from0 == long.class || from1 == long.class) {
                return long.class;
            } else {
                if (from0 == byte.class) {
                    if (from1 == byte.class) {
                        return byte.class;
                    } else if (from1 == short.class) {
                        if (const1 != null) {
                            final short constant = (short)const1;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return byte.class;
                            }
                        }

                        return short.class;
                    } else if (from1 == char.class) {
                        return int.class;
                    } else if (from1 == int.class) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return byte.class;
                            }
                        }

                        return int.class;
                    }
                } else if (from0 == short.class) {
                    if (from1 == byte.class) {
                        if (const0 != null) {
                            final short constant = (short)const0;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return byte.class;
                            }
                        }

                        return short.class;
                    } else if (from1 == short.class) {
                        return short.class;
                    } else if (from1 == char.class) {
                        return int.class;
                    } else if (from1 == int.class) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Short.MAX_VALUE && constant >= Short.MIN_VALUE) {
                                return short.class;
                            }
                        }

                        return int.class;
                    }
                } else if (from0 == char.class) {
                    if (from1 == byte.class) {
                        return int.class;
                    } else if (from1 == short.class) {
                        return int.class;
                    } else if (from1 == char.class) {
                        return char.class;
                    } else if (from1 == int.class) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Character.MAX_VALUE && constant >= Character.MIN_VALUE) {
                                return byte.class;
                            }
                        }

                        return int.class;
                    }
                } else if (from0 == int.class) {
                    if (from1 == byte.class) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return byte.class;
                            }
                        }

                        return int.class;
                    } else if (from1 == short.class) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Short.MAX_VALUE && constant >= Short.MIN_VALUE) {
                                return byte.class;
                            }
                        }

                        return int.class;
                    } else if (from1 == char.class) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Character.MAX_VALUE && constant >= Character.MIN_VALUE) {
                                return byte.class;
                            }
                        }

                        return int.class;
                    } else if (from1 == int.class) {
                        return int.class;
                    }
                }
            }
        }

        // TODO: In the rare case we still haven't reached a correct promotion we need
        // TODO: to calculate the highest upper bound for the two types and return that.
        // TODO: However, for now we just return objectType that may require an extra cast.

        return Object.class;
    }

    private AnalyzerCaster() {

    }
}
