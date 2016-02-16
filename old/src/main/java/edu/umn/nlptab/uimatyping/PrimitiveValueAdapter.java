/*
 * Copyright (c) 2015 Regents of the University of Minnesota.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umn.nlptab.uimatyping;

import edu.umn.nlptab.NlpTabException;
import edu.umn.nlptab.core.UimaPrimitive;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.TypeSystem;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Internal class for adapting primitive values.
 */
class PrimitiveValueAdapter implements ValueAdapter {
    private final Method getMethod;
    private final String key;

    /**
     * Default constructor. Initializes the reflect method needed to retrieve the value of the list, and a key for the
     * uima primitive.
     *
     * @param getMethod reflect method to get the value of the array
     * @param key       key to pass to the create UimaPrimitive
     */
    PrimitiveValueAdapter(Method getMethod, String key) {
        this.getMethod = getMethod;
        this.key = key;
    }

    @Override
    public UimaPrimitive getValueOfFeature(TypeSystem typeSystem, Feature feature, FeatureStructure featureStructure) throws NlpTabException {
        Object invoke;
        try {
            invoke = getMethod.invoke(featureStructure, feature);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new NlpTabException(e);
        }
        return new UimaPrimitive(invoke, key);
    }

    @Override
    public UimaPrimitive getValueOfFS(TypeSystem typeSystem, FeatureStructure targetFS) throws NlpTabException {
        throw new UnsupportedOperationException();
    }
}
