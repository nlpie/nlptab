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

package edu.umn.nlptab.systemindex;

import edu.umn.nlptab.casprocessing.FeatureStructureProcessorDelegate;
import edu.umn.nlptab.casprocessing.PrimitiveFeatureInstance;
import edu.umn.nlptab.casprocessing.ReferenceFeatureInstance;
import edu.umn.nlptab.casprocessing.SofaData;
import edu.umn.nlptab.NlpTabException;
import edu.umn.nlptab.core.UimaPrimitive;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.TypeSystem;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * Class which is responsible for uploading the various metadata components of a FeatureStructure to ElasticSearch.
 */
class SystemIndexFSProcessorDelegate implements FeatureStructureProcessorDelegate {
    private final Client client;

    @Nullable
    private UimaPrimitive primitiveValue;

    @Nullable
    private List<String> listItems;

    @Nullable
    private List<String> arrayItems;

    @Nullable
    private List<ReferenceFeatureInstance> referenceFeatureInstances;

    @Nullable
    private Map<String, Collection<PrimitiveFeatureInstance>> primitiveFeatureInstances;

    @Inject
    SystemIndexFSProcessorDelegate(Client client) {
        this.client = client;
    }

    @Override
    public void setPrimitiveValue(UimaPrimitive primitiveValue) {
        this.primitiveValue = primitiveValue;
    }

    @Override
    public void addListItem(String identifierForFs) {
        if (listItems == null) {
            listItems = new ArrayList<>();
        }
        listItems.add(identifierForFs);
    }

    @Override
    public void addArrayItem(String identifierForFs) {
        if (arrayItems == null) {
            arrayItems = new ArrayList<>();
        }
        arrayItems.add(identifierForFs);
    }

    @Override
    public void addReferenceFeatureInstance(ReferenceFeatureInstance referenceFeatureInstance) {
        if (referenceFeatureInstances == null) {
            referenceFeatureInstances = new ArrayList<>();
        }
        referenceFeatureInstances.add(referenceFeatureInstance);
    }

    @Override
    public void addPrimitiveFeatureInstance(PrimitiveFeatureInstance primitiveFeatureInstance) {
        if (primitiveFeatureInstances == null) {
            primitiveFeatureInstances = new HashMap<>();
        }
        primitiveFeatureInstances.compute(primitiveFeatureInstance.getFeatureValueKey(), (key, value) -> {
            if (value == null) {
                value = new ArrayList<>();
            }
            value.add(primitiveFeatureInstance);
            return value;
        });
    }

    @Override
    public IndexRequestBuilder buildRequest(String primaryIndex, SofaData sofaData, FeatureStructure featureStructure) throws IOException, NlpTabException, InterruptedException {
        Type type = featureStructure.getType();
        if (type == null) {
            throw new IllegalStateException();
        }

        CAS cas = featureStructure.getCAS();
        TypeSystem typeSystem = cas.getTypeSystem();
        if (typeSystem == null) {
            throw new IllegalStateException();
        }
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("system", primaryIndex)
                .field("casIdentifier", sofaData.getCasIdentifierString())
                .field("casViewIdentifier", sofaData.getCasViewIdentifierString())
                .field("documentIdentifier", sofaData.getDocumentIdentifierString())
                .field("primaryType", type.getName());

        builder.startArray("types");
        Type parentType = type;
        while (parentType != null) {
            builder.value(parentType.getName());
            parentType = typeSystem.getParent(parentType);
        }
        builder.endArray();

        if (primitiveValue != null) {
            builder.field("items", primitiveValue.getValueOrNull());
        }

        if (listItems != null) {
            builder.array("listItems", listItems.toArray());
        }

        if (arrayItems != null) {
            builder.array("arrayItems", arrayItems.toArray());
        }

        if (primitiveFeatureInstances != null) {
            Set<String> primitiveFeatureInstanceIds = primitiveFeatureInstances.keySet();
            for (String valueTypeKey : primitiveFeatureInstanceIds) {
                builder.startObject(valueTypeKey + "Features");
                Collection<PrimitiveFeatureInstance> valuePrimitiveFeatures = primitiveFeatureInstances.get(valueTypeKey);
                if (valuePrimitiveFeatures != null) {
                    for (PrimitiveFeatureInstance primitiveFeatureInstance : valuePrimitiveFeatures) {
                        String name = primitiveFeatureInstance.getLuceneSafeFeatureName();
                        Object valueOrNull = primitiveFeatureInstance.getValueOfFeature().getValueOrNull();
                        builder.field(name, valueOrNull);
                    }
                }
                builder.endObject();
            }
        }

        if (referenceFeatureInstances != null) {
            builder.startObject("references");
            for (ReferenceFeatureInstance referenceFeatureInstance : referenceFeatureInstances) {
                String featureName = referenceFeatureInstance.getLuceneSafeFeatureName();
                String referenceId = referenceFeatureInstance.getReferenceId();
                builder.field(featureName, referenceId);
            }
            builder.endObject();
        }

        return client.prepareIndex(primaryIndex, "FeatureStructure")
                .setId(sofaData.getIdentifierForFs(featureStructure))
                .setSource(builder.endObject());
    }


}
