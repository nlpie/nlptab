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

package edu.umn.nlptab.casprocessing;

import edu.umn.nlptab.uimatyping.TypeSystemInfo;
import org.elasticsearch.common.inject.Provider;

/**
 *
 */
public class CasProcessorSettings {

    private final String primaryIndex;

    private final TypeSystemInfo typeSystemInfo;

    private final Provider<? extends FeatureStructureProcessorDelegate> featureStructureProcessorDelegateProvider;

    private final CasProcessingDelegate casProcessingDelegate;

    public CasProcessorSettings(String primaryIndex,
                                TypeSystemInfo typeSystemInfo,
                                Provider<? extends FeatureStructureProcessorDelegate> featureStructureProcessorDelegateProvider,
                                CasProcessingDelegate casProcessingDelegate) {
        this.primaryIndex = primaryIndex;
        this.typeSystemInfo = typeSystemInfo;
        this.featureStructureProcessorDelegateProvider = featureStructureProcessorDelegateProvider;
        this.casProcessingDelegate = casProcessingDelegate;
    }

    public String getPrimaryIndex() {
        return primaryIndex;
    }

    public TypeSystemInfo getTypeSystemInfo() {
        return typeSystemInfo;
    }

    public FeatureStructureProcessorDelegate createFeatureStructureProcessorDelegate() {
        return featureStructureProcessorDelegateProvider.get();
    }

    public CasProcessingDelegate getCasProcessingDelegate() {
        return casProcessingDelegate;
    }
}
