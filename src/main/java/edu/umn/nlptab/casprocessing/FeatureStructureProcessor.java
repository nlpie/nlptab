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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import edu.umn.nlptab.NlpTabException;
import edu.umn.nlptab.core.UimaPrimitive;
import edu.umn.nlptab.uimatyping.FeaturesForType;
import edu.umn.nlptab.uimatyping.TypeSystemInfo;
import edu.umn.nlptab.uimatyping.ValueAdapter;
import org.apache.uima.cas.*;
import org.apache.uima.cas.text.AnnotationFS;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.assistedinject.Assisted;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 *
 */
public class FeatureStructureProcessor {
    private static final ImmutableSet<String> blackListedFeatures = ImmutableSet.of(CAS.FEATURE_BASE_NAME_HEAD,
            CAS.FEATURE_BASE_NAME_TAIL, CAS.FEATURE_BASE_NAME_SOFA);

    private final Provider<PrimitiveFeatureInstance> primitiveFeatureInstanceProvider;

    private final CasProcessorSettings casProcessorSettings;

    private final TypeSystemInfo typeSystemInfo;

    private final FeatureStructureProcessorDelegate featureStructureProcessorDelegate;

    private final SofaData sofaData;

    private final FeatureStructure featureStructure;

    @Inject
    FeatureStructureProcessor(Provider<PrimitiveFeatureInstance> primitiveFeatureInstanceProvider,
                              @Assisted CasProcessorSettings casProcessorSettings,
                              @Assisted SofaData sofaData,
                              @Assisted FeatureStructure featureStructure) {
        this.primitiveFeatureInstanceProvider = primitiveFeatureInstanceProvider;
        this.casProcessorSettings = casProcessorSettings;
        typeSystemInfo = casProcessorSettings.getTypeSystemInfo();
        featureStructureProcessorDelegate = casProcessorSettings.createFeatureStructureProcessorDelegate();
        this.sofaData = sofaData;
        this.featureStructure = featureStructure;
    }

    @Nullable
    IndexRequestBuilder process() throws IOException, NlpTabException, InterruptedException {
        String identifierForFs = sofaData.getIdentifierForFs(featureStructure);

        if (featureStructure instanceof AnnotationFS) {
            AnnotationFS annotationFS = (AnnotationFS) featureStructure;
            sofaData.addLocation(identifierForFs, new FsDocumentLocation(annotationFS.getBegin(), annotationFS.getEnd()));
        }

        TypeSystem typeSystem = featureStructure.getCAS().getTypeSystem();
        Type type = featureStructure.getType();

        ValueAdapter valueAdapter = typeSystemInfo.getValueAdapter(type);
        if (valueAdapter != null) {
            // is primitive collection
            UimaPrimitive primitiveValue = valueAdapter.getValueOfFS(typeSystem, featureStructure);
            featureStructureProcessorDelegate.setPrimitiveValue(primitiveValue);
        } else if (typeSystem.subsumes(typeSystem.getType(CAS.TYPE_NAME_FS_LIST), type)) {
            // is fs list
            Feature head = typeSystem.getFeatureByFullName(CAS.FEATURE_FULL_NAME_FS_LIST_HEAD);
            Feature tail = typeSystem.getFeatureByFullName(CAS.FEATURE_FULL_NAME_FS_LIST_TAIL);

            FeatureStructure cons = featureStructure;
            while (cons.getType().getName().equals(CAS.TYPE_NAME_NON_EMPTY_FS_LIST)) {
                FeatureStructure childFS = cons.getFeatureValue(head);

                String childIdentifier = sofaData.getIdentifierForFs(childFS);

                sofaData.markAsChild(childIdentifier, identifierForFs);

                featureStructureProcessorDelegate.addListItem(childIdentifier);

                cons = cons.getFeatureValue(tail);
            }
        } else if (featureStructure instanceof ArrayFS) {
            // is array fs add children
            ArrayFS arrayFS = (ArrayFS) featureStructure;

            for (int i = 0; i < arrayFS.size(); i++) {
                FeatureStructure arrayItem = arrayFS.get(i);
                if (arrayItem != null) {
                    String childIdentifier = sofaData.getIdentifierForFs(arrayItem);
                    sofaData.markAsChild(childIdentifier, identifierForFs);
                    featureStructureProcessorDelegate.addArrayItem(childIdentifier);
                }
            }
        } else {
            // is normal fs, add features
            FeaturesForType featuresForType = typeSystemInfo.getFeaturesForType(type);

            for (Feature feature : featuresForType.getReferenceFeatures()) {
                FeatureStructure childFs = featureStructure.getFeatureValue(feature);

                String featureShortName = feature.getShortName();
                if (!blackListedFeatures.contains(featureShortName)) {
                    ReferenceFeatureInstance referenceFeatureInstance = new ReferenceFeatureInstance();
                    referenceFeatureInstance.setFeatureName(feature.getName());
                    if (childFs != null) {
                        String childIdentifier = sofaData.getIdentifierForFs(childFs);
                        referenceFeatureInstance.setReferenceId(childIdentifier);

                        sofaData.markAsChild(childIdentifier, identifierForFs);
                    }
                    featureStructureProcessorDelegate.addReferenceFeatureInstance(referenceFeatureInstance);
                }
            }

            for (Feature feature : featuresForType.getPrimitiveFeatures()) {
                String primitiveFeatureName = feature.getName();
                String featureShortName = feature.getShortName();
                if (!blackListedFeatures.contains(featureShortName)) {
                    ValueAdapter featureValueAdapter = typeSystemInfo.getValueAdapter(feature.getRange());
                    assert featureValueAdapter != null;
                    UimaPrimitive valueOfFeature = featureValueAdapter.getValueOfFeature(typeSystem, feature, featureStructure);

                    Preconditions.checkNotNull(valueOfFeature);

                    PrimitiveFeatureInstance primitiveFeatureInstance = primitiveFeatureInstanceProvider.get();

                    primitiveFeatureInstance.setFeatureName(primitiveFeatureName);
                    primitiveFeatureInstance.setValueOfFeature(valueOfFeature);

                    featureStructureProcessorDelegate.addPrimitiveFeatureInstance(primitiveFeatureInstance);
                }
            }
        }

        return featureStructureProcessorDelegate.buildRequest(casProcessorSettings.getPrimaryIndex(), sofaData,
                featureStructure);
    }
}
