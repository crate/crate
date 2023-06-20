/**
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.crate.common.concurrent;

import java.util.function.DoubleUnaryOperator;

/**
 * Contract for tracking a measurement such as a minimum or average of a sample set
 */
public interface Measurement {
    /**
     * Add a single sample and update the internal state.
     * @param sample
     * @return True if internal state was updated
     */
    double add(double sample);

    /**
     * @return Return the current value
     */
    double get();

    /**
     * Reset the internal state as if no samples were ever added
     */
    void reset();

    void update(DoubleUnaryOperator operation);
}
