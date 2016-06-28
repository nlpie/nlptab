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

import edu.umn.nlptab.core.InstanceIndexes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.xcontent.XContentBuilder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Configuration for an analysis task.
 *
 * @author Ben Knoll
 * @since 1.0
 */
public class AnalysisConfig {
    private final Provider<FeatureValueMapping> featureValueMappingProvider;

    private final Provider<UnitOfAnalysis> unitOfAnalysisProvider;

    @Nullable
    private Collection<FeatureValueMapping> featureValueMappings;

    @Nullable
    private UnitOfAnalysis hypothesis;

    @Nullable
    private UnitOfAnalysis reference;

    @Nullable
    private InstanceIndexes instanceIndexes;

    @Nullable
    private String description;

    private int fuzzDistance;

    private boolean hitMiss;

    @Inject
    public AnalysisConfig(Provider<FeatureValueMapping> featureValueMappingProvider,
                          Provider<UnitOfAnalysis> unitOfAnalysisProvider) {
        this.featureValueMappingProvider = featureValueMappingProvider;
        this.unitOfAnalysisProvider = unitOfAnalysisProvider;
    }

    public void initFromMap(Map<String, Object> jsonMap) throws AnalysisConfigurationException {
        @SuppressWarnings("unchecked")
        Map<String, Object> hypothesisUnitOfAnalysisMap = (Map<String, Object>) jsonMap.get("hypothesisUnitOfAnalysis");
        if (hypothesisUnitOfAnalysisMap == null) {
            throw new AnalysisConfigurationException("null first unit of analysis");
        }
        hypothesis = unitOfAnalysisProvider.get();
        hypothesis.initFromJsonMap(hypothesisUnitOfAnalysisMap);

        @SuppressWarnings("unchecked")
        Map<String, Object> secondUnitOfAnalysisMap = (Map<String, Object>) jsonMap.get("referenceUnitOfAnalysis");
        if (secondUnitOfAnalysisMap == null) {
            throw new AnalysisConfigurationException("null second unit of analysis");
        }
        reference = unitOfAnalysisProvider.get();
        reference.initFromJsonMap(secondUnitOfAnalysisMap);

        String instance = (String) jsonMap.get("instance");
        if (instance == null) {
            throw new AnalysisConfigurationException("null instance");
        }
        this.instanceIndexes = new InstanceIndexes(instance);

        String description = (String) jsonMap.get("description");
        if (description == null) {
            throw new AnalysisConfigurationException("null description");
        }
        this.description = description;

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> featureValueMappingJsonMaps = (List<Map<String, Object>>) jsonMap.get("featureValueMappings");
        if (featureValueMappingJsonMaps == null) {
            throw new AnalysisConfigurationException("null feature value mappings");
        }
        featureValueMappings = new ArrayList<>();
        for (Map<String, Object> featureValueMappingJsonMap : featureValueMappingJsonMaps) {
            FeatureValueMapping featureValueMapping = featureValueMappingProvider.get()
                    .initFromJsonMap(featureValueMappingJsonMap, hypothesis.getSystemIndex(), reference.getSystemIndex());
            featureValueMappings.add(featureValueMapping);
        }

        Integer fuzzDistance = (Integer) jsonMap.get("fuzzDistance");

        if (fuzzDistance == null) {
            throw new AnalysisConfigurationException("fuzzDistance was null");
        }

        this.fuzzDistance = fuzzDistance;

        String hitMiss = (String) jsonMap.get("hitMiss");

        if (hitMiss == null) {
            throw new AnalysisConfigurationException("hitMiss was null");
        }

        this.hitMiss = Boolean.parseBoolean(hitMiss);
    }

    boolean isHitMiss() {
        return hitMiss;
    }

    UnitOfAnalysis getHypothesis() {
        if (hypothesis == null) {
            throw new IllegalStateException("hypothesis not initialized");
        }
        return hypothesis;
    }

    UnitOfAnalysis getReference() {
        if (reference == null) {
            throw new IllegalStateException("reference not initialized");
        }
        return reference;
    }

    InstanceIndexes getInstanceIndexes() {
        if (instanceIndexes == null) {
            throw new IllegalStateException("instanceIndexes not initialized");
        }
        return instanceIndexes;
    }

    int getFuzzDistance() {
        return fuzzDistance;
    }

    Collection<FeatureValueTester> createFeatureStructureTesters(Map<String, Object> hypothesisFeatureStructure) {
        if (featureValueMappings == null) {
            throw new IllegalStateException("featureValueMappings not initialized");
        }

        Collection<FeatureValueTester> featureStructureTesters = new ArrayList<>();

        for (FeatureValueMapping featureValueMapping : featureValueMappings) {
            FeatureValueTester tester = featureValueMapping.createTester(hypothesisFeatureStructure);
            featureStructureTesters.add(tester);
        }

        return featureStructureTesters;
    }

    Collection<FeatureValueTester> createConverseFeatureStructureTesters(Map<String, Object> referenceFeatureStructure) {
        if (featureValueMappings == null) {
            throw new IllegalStateException("featureValueMappings not initialized");
        }
        Collection<FeatureValueTester> featureStructureTesters = new ArrayList<>();

        for (FeatureValueMapping featureValueMapping : featureValueMappings) {
            FeatureValueTester tester = featureValueMapping.createConverseTester(referenceFeatureStructure);
            featureStructureTesters.add(tester);
        }

        return featureStructureTesters;
    }

    void append(XContentBuilder xContentBuilder) throws IOException {
        if (hypothesis == null) {
            throw new IllegalStateException("hypothesis not initialized");
        }
        if (reference == null) {
            throw new IllegalStateException("reference not initialized");
        }
        if (featureValueMappings == null) {
            throw new IllegalStateException("featureValueMappings not initialized");
        }

        xContentBuilder.startObject("hypothesisUnitOfAnalysis");
        hypothesis.appendTo(xContentBuilder);
        xContentBuilder.endObject();
        xContentBuilder.startObject("referenceUnitOfAnalysis");
        reference.appendTo(xContentBuilder);
        xContentBuilder.endObject();

        xContentBuilder.field("description", description);

        xContentBuilder.startArray("featureValueMappings");

        for (FeatureValueMapping featureValueMapping : featureValueMappings) {
            featureValueMapping.addToXContent(xContentBuilder);
        }

        xContentBuilder.endArray();
    }
}
