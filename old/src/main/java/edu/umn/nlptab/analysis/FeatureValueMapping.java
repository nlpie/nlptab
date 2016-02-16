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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 *
 */
class FeatureValueMapping {
    private final Provider<Feature> featureProvider;

    private final Provider<FeatureValueTester> featureValueTesterProvider;

    private Feature hypothesisFeature;

    private Feature referenceFeature;

    private EquivalenceTest equivalenceTest;

    private Map<Object, Set<Object>> valueMappings;

    private Map<Object, Set<Object>> converseValueMappings;

    @Inject
    FeatureValueMapping(Provider<Feature> featureProvider, Provider<FeatureValueTester> featureValueTesterProvider) {
        this.featureProvider = featureProvider;
        this.featureValueTesterProvider = featureValueTesterProvider;
    }

    FeatureValueMapping initFromJsonMap(Map<String, Object> jsonMap, String hypothesisSystemIndex, String referenceSystemIndex) throws AnalysisConfigurationException {
        hypothesisFeature = featureProvider.get();
        @SuppressWarnings("unchecked")
        Map<String, Object> hypothesisFeatureJson = (Map<String, Object>) jsonMap.get("hypothesisFeature");
        hypothesisFeature.initFromJsonMap(hypothesisFeatureJson);
        hypothesisFeature.withSystemIndex(hypothesisSystemIndex);

        referenceFeature = featureProvider.get();
        @SuppressWarnings("unchecked")
        Map<String, Object> referenceFeatureJson = (Map<String, Object>) jsonMap.get("referenceFeature");
        referenceFeature.initFromJsonMap(referenceFeatureJson);
        referenceFeature.withSystemIndex(referenceSystemIndex);

        String equivalence = (String) jsonMap.get("equivalence");
        if (equivalence == null) {
            throw new AnalysisConfigurationException("equivalence was null");
        }
        equivalenceTest = EquivalenceTests.getEquivalenceTest(equivalence);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> valueMappingsJson = (List<Map<String, Object>>) jsonMap.get("valueMappings");
        if (valueMappingsJson == null) {
            throw new AnalysisConfigurationException("Value mappings was null");
        }


        valueMappings = new HashMap<>();
        for (Map<String, Object> valueMappingJson : valueMappingsJson) {
            Object from = valueMappingJson.get("from");
            Object to = valueMappingJson.get("to");

            valueMappings.compute(from, (key, values) -> {
                if (values == null) {
                    values = new HashSet<>();
                }

                values.add(to);

                return values;
            });
        }

        converseValueMappings = valueMappings.entrySet()
                .stream()
                .flatMap(entry -> entry.getValue()
                        .stream()
                        .map(value -> new AbstractMap.SimpleImmutableEntry<>(value, entry.getKey())))
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> {
                            Set<Object> set = new HashSet<>();
                            set.add(entry.getValue());
                            return set;
                        },
                        (first, second) -> {
                            first.addAll(second);
                            return first;
                        }));

        return this;
    }

    FeatureValueTester createTester(Map<String, Object> hypothesisFeatureStructure) {
        Object value = hypothesisFeature.getValueFromFeatureStructure(hypothesisFeatureStructure);
        Set<Object> mappedValue = valueMappings.containsKey(value) ? valueMappings.get(value) : Collections.singleton(value);
        return featureValueTesterProvider.get()
                .withEquivalenceTest(equivalenceTest)
                .withHypothesisValues(mappedValue)
                .withReferenceFeature(referenceFeature);
    }

    FeatureValueTester createConverseTester(Map<String, Object> referenceFeatureStructure) {
        Object value = referenceFeature.getValueFromFeatureStructure(referenceFeatureStructure);
        Set<Object> mappedValue = converseValueMappings.containsKey(value) ? converseValueMappings.get(value) : Collections.singleton(value);
        return featureValueTesterProvider.get()
                .withEquivalenceTest(EquivalenceTests.getConverse(equivalenceTest))
                .withHypothesisValues(mappedValue)
                .withReferenceFeature(hypothesisFeature);
    }

    public void addToXContent(XContentBuilder xContentBuilder) throws IOException {
        xContentBuilder.startObject();

        xContentBuilder.startObject("hypothesisFeature");

        hypothesisFeature.addToXContent(xContentBuilder);

        xContentBuilder.endObject();

        xContentBuilder.startObject("referenceFeature");

        referenceFeature.addToXContent(xContentBuilder);

        xContentBuilder.endObject();

        xContentBuilder.endObject();
    }
}
