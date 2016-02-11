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
import org.apache.uima.cas.Type;
import org.apache.uima.cas.TypeSystem;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Internal enumerated class responsible for adapting primitive arrays.
 */
enum PrimitiveArrayValueAdapter implements ValueAdapter {
    /**
     * Primitive arrays don't need different accessors depending on their type so this is implemented as a enumeration
     * singleton.
     */
    INSTANCE {
        @Override
        public UimaPrimitive getValueOfFS(TypeSystem typeSystem, FeatureStructure targetFS) throws NlpTabException {
            Class<? extends FeatureStructure> targetFSClass = targetFS.getClass();
            Method sizeMethod;
            try {
                sizeMethod = targetFSClass.getMethod("size");
            } catch (NoSuchMethodException e) {
                throw new NlpTabException(e);
            }
            Object invoke;
            try {
                invoke = sizeMethod.invoke(targetFS);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new NlpTabException(e);
            }
            assert invoke instanceof Integer;
            Method toArrayMethod;
            try {
                toArrayMethod = targetFSClass.getMethod("toArray");
            } catch (NoSuchMethodException e) {
                throw new NlpTabException(e);
            }
            Object result;
            try {
                result = toArrayMethod.invoke(targetFS);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new NlpTabException(e);
            }

            Type type = targetFS.getType();
            String typeShortName = type.getShortName();

            return new UimaPrimitive(result, typeShortName);
        }

        @Override
        public UimaPrimitive getValueOfFeature(TypeSystem typeSystem, Feature feature, FeatureStructure featureStructure) throws NlpTabException {
            FeatureStructure targetFS = featureStructure.getFeatureValue(feature);

            UimaPrimitive uimaPrimitive;
            if (targetFS != null) {
                uimaPrimitive = getValueOfFS(typeSystem, targetFS);
            } else {
                Type range = feature.getRange();
                String rangeShortName = range.getShortName();
                uimaPrimitive = new UimaPrimitive(null, rangeShortName);
            }
            return uimaPrimitive;
        }
    }
}
