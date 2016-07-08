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

package edu.umn.nlptab.analysis;

import java.util.Map;
import java.util.Set;

/**
 *
 */
class FeatureValueTester {
    private Set<Object> mappedValues;

    private EquivalenceTest equivalenceTest;

    private Feature referenceFeature;

    FeatureValueTester withHypothesisValues(Set<Object> hypothesisValues) {
        this.mappedValues = hypothesisValues;
        return this;
    }

    FeatureValueTester withEquivalenceTest(EquivalenceTest equivalenceTest) {
        this.equivalenceTest = equivalenceTest;
        return this;
    }

    FeatureValueTester withReferenceFeature(Feature referenceFeature) {
        this.referenceFeature = referenceFeature;
        return this;
    }

    public boolean test(Map<String, Object> featureStructure) {
        Object value = getReferenceValue(featureStructure);
        return mappedValues.stream().anyMatch(mappedValue -> equivalenceTest.test(mappedValue, value));
    }

    public Object getReferenceValue(Map<String, Object> featureStructure) {
        return referenceFeature.getValueFromFeatureStructure(featureStructure);
    }

    public Set<Object> getMappedValues() {
        return mappedValues;
    }
}
