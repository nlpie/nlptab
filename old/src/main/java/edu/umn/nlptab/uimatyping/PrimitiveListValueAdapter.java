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
import java.util.Collection;
import java.util.LinkedList;

/**
 * Internal class responsible for adapting uima primitive lists.
 */
class PrimitiveListValueAdapter implements ValueAdapter {
    private final Feature headFeature;
    private final Method headMethod;
    private final String nonEmptyName;
    private final Feature tailFeature;

    /**
     * Default constructor. Initializes the variables necessary for iterating uima lists.
     *
     * @param headFeature  the feature for the head on the list node. Retrieved using CAS.FEATURE_BASE_NAME_HEAD
     * @param headMethod   the reflect method to call to retrieve the head value.
     * @param nonEmptyName the qualified type name for the NonEmpty version of this list type ex: {@link org.apache.uima.jcas.cas.NonEmptyFloatList}
     * @param tailFeature  the feature for the tail on the list node. Retrieved using CAS.FEATURE_BASE_NAME_TAIL
     */
    PrimitiveListValueAdapter(Feature headFeature, Method headMethod, String nonEmptyName, Feature tailFeature) {
        this.headFeature = headFeature;
        this.headMethod = headMethod;
        this.nonEmptyName = nonEmptyName;
        this.tailFeature = tailFeature;
    }

    @Override
    public UimaPrimitive getValueOfFS(TypeSystem typeSystem, FeatureStructure targetFS) throws NlpTabException {
        Collection<Object> values = new LinkedList<>();

        FeatureStructure pointer = targetFS;

        while (pointer.getType().getName().equals(nonEmptyName)) {
            Object value;
            try {
                value = headMethod.invoke(pointer, headFeature);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new NlpTabException(e);
            }

            values.add(value);

            pointer = pointer.getFeatureValue(tailFeature);
        }

        Type type = targetFS.getType();
        String typeShortName = type.getShortName();

        return new UimaPrimitive(values, typeShortName);
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
