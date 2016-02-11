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

/**
 * An interface for classes which are responsible for retrieving the primitive value of a Uima primitive. This can
 * either be a {@link org.apache.uima.cas.FeatureStructure} (lists or arrays of primitives)
 * or a {@link org.apache.uima.cas.Feature} (lists or arrays of primitives, or primitives).
 */
public interface ValueAdapter {
    /**
     * Returns the value of a FeatureStructure
     *
     * @param typeSystem
     * @param targetFS FeatureStructure to convert to a primitive
     * @return UimaPrimitive representation of the feature structure.
     * @throws NlpTabException
     */
    UimaPrimitive getValueOfFS(TypeSystem typeSystem, FeatureStructure targetFS) throws NlpTabException;

    /**
     *
     * @param typeSystem
     * @param feature
     * @param featureStructure
     * @return
     * @throws NlpTabException
     */
    UimaPrimitive getValueOfFeature(TypeSystem typeSystem, Feature feature, FeatureStructure featureStructure) throws NlpTabException;
}
