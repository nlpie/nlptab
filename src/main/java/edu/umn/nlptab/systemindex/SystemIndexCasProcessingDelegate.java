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

import edu.umn.nlptab.casprocessing.CasProcessingDelegate;
import edu.umn.nlptab.casprocessing.FsDocumentLocation;
import edu.umn.nlptab.casprocessing.SofaData;
import edu.umn.nlptab.NlpTabException;
import edu.umn.nlptab.core.InstanceIndexes;
import edu.umn.nlptab.uimatyping.FeaturesForType;
import edu.umn.nlptab.uimatyping.TypeSystemInfo;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.TypeSystem;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Responsible for performing the updates of adding document locations
 *
 * @author Ben Knoll
 * @since 0.4.0
 */
class SystemIndexCasProcessingDelegate implements CasProcessingDelegate {
    public static class Factory {
        private final Client client;

        @Inject
        Factory(Client client) {
            this.client = client;
        }

        public SystemIndexCasProcessingDelegate create(InstanceIndexes instanceIndexes, String systemIndex) {
            return new SystemIndexCasProcessingDelegate(client, instanceIndexes, systemIndex);
        }
    }

    private static final ESLogger LOGGER = Loggers.getLogger(SystemIndexCasProcessingDelegate.class);

    private final Client client;

    private final Set<String> typesSeen;

    private final String searchIndex;

    private final String systemIndex;

    private final ListenableActionFuture<GetResponse> systemIndexFuture;

    @Nullable
    private List<String> ignoredViews;

    private SystemIndexCasProcessingDelegate(Client client, InstanceIndexes instanceIndexes, String systemIndex) {
        this.client = client;
        typesSeen = new HashSet<>();

        searchIndex = instanceIndexes.searchIndex();

        this.systemIndex = systemIndex;

        systemIndexFuture = client.prepareGet(instanceIndexes.metadataIndex(), "SystemIndex", systemIndex)
                .execute();
    }

    @Override
    public boolean shouldProcessView(String viewName) throws NlpTabException {
        if (ignoredViews == null) {
            try {
                GetResponse getResponse = systemIndexFuture.get();
                Map<String, Object> systemIndexMetadata = getResponse.getSource();
                ignoredViews = (List<String>) systemIndexMetadata.get("ignoredViews");
            } catch (InterruptedException | ExecutionException e) {
                throw new NlpTabException(e);
            }
        }

        return !ignoredViews.contains(viewName);
    }

    @Override
    public void priorToProcessing(SofaData sofaData) throws NlpTabException {
        String documentIdentifierString = sofaData.getDocumentIdentifierString();
        String documentText = sofaData.getDocumentText();
        try {
            client.prepareIndex(searchIndex, "Document")
                    .setId(documentIdentifierString)
                    .setSource(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("identifier", documentIdentifierString)
                            .field("text", documentText)
                            .field("length", documentText == null ? 0 : documentText.length())
                            .endObject())
                    .execute()
                    .actionGet();
        } catch (IOException e) {
            throw new NlpTabException(e);
        }

        try {
            client.prepareIndex(searchIndex, "DocumentInSystem")
                    .setRouting(documentIdentifierString)
                    .setParent(documentIdentifierString)
                    .setSource(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("systemIndex", systemIndex)
                            .field("casIdentifier", sofaData.getCasIdentifierString())
                            .field("casViewIdentifier", sofaData.getCasViewIdentifierString())
                            .field("documentIdentifier", documentIdentifierString)
                            .endObject())
                    .execute()
                    .actionGet();
        } catch (IOException e) {
            throw new NlpTabException(e);
        }
    }

    @Override
    public void willProcess(FeatureStructure featureStructure, TypeSystemInfo typeSystemInfo) throws NlpTabException {
        Type type = featureStructure.getType();
        TypeSystem typeSystem = featureStructure.getCAS().getTypeSystem();

        Type typePointer = type;
        while (typePointer != null && !typesSeen.contains(typePointer.getName())) {
            typesSeen.add(typePointer.getName());

            uploadType(typeSystemInfo, typeSystem, typePointer);

            typePointer = typeSystem.getParent(typePointer);
        }
    }

    private void uploadType(TypeSystemInfo typeSystemInfo, TypeSystem typeSystem, Type typePointer) throws NlpTabException {
        try {
            String typeName = typePointer.getName();
            LOGGER.debug("Encountered new type: {}, uploading to server", typeName);
            String typeShortName = typePointer.getShortName();

            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("typeName", typeName)
                    .field("typeShortName", typeShortName)
                    .startArray("primitiveFeatures");

            FeaturesForType featuresForType = typeSystemInfo.getFeaturesForType(typePointer);

            for (Feature feature : featuresForType.getPrimitiveFeatures()) {
                addFeatureToXContent(xContentBuilder, feature);
            }

            xContentBuilder.endArray()
                    .startArray("referenceFeatures");

            for (Feature feature : featuresForType.getReferenceFeatures()) {
                addFeatureToXContent(xContentBuilder, feature);
            }

            xContentBuilder.endArray()
                    .startArray("parentTypes");

            Type parentType = typeSystem.getParent(typePointer);
            while (parentType != null) {
                String parentTypeName = parentType.getName();
                xContentBuilder.value(parentTypeName);
                parentType = typeSystem.getParent(parentType);
            }

            xContentBuilder.endArray()
                    .endObject();

            client.prepareIndex(systemIndex, "Type")
                    .setSource(xContentBuilder)
                    .get();
        } catch (IOException e) {
            throw new NlpTabException(e);
        }
    }

    private static void addFeatureToXContent(XContentBuilder xContentBuilder, Feature feature) throws IOException {
        xContentBuilder.startObject()
                .field("name", feature.getName())
                .field("shortName", feature.getShortName())
                .field("valueType", feature.getRange().getName())
                .endObject();
    }

    @Override
    public void afterProcessing(SofaData sofaData) throws InterruptedException, IOException {
        Map<String, Collection<String>> childToParentMap = sofaData.getChildToParentMap();
        Map<String, FsDocumentLocation> documentLocationMap = sofaData.getDocumentLocationMap();

        Set<String> directKeys = documentLocationMap.keySet();
        Set<String> childKeys = childToParentMap.keySet();

        Set<String> allKeys = new HashSet<>();
        allKeys.addAll(directKeys);
        allKeys.addAll(childKeys);

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (String key : allKeys) {
            DocumentLocationsForDocument documentLocationsForDocument = new DocumentLocationsForDocument(childToParentMap, documentLocationMap, key);
            documentLocationsForDocument.invoke();
            SortedMap<Integer, Set<FsDocumentLocation>> locationAtDistance = documentLocationsForDocument.getLocationAtDistance();

            if (!locationAtDistance.entrySet().isEmpty()) {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                        .startArray("documentLocations");

                Set<Integer> distances = locationAtDistance.keySet();
                for (Integer distance : distances) {
                    Set<FsDocumentLocation> fsDocumentLocations = locationAtDistance.get(distance);
                    if (fsDocumentLocations == null || fsDocumentLocations.size() == 0) {
                        continue;
                    }

                    for (FsDocumentLocation fsDocumentLocation : fsDocumentLocations) {
                        builder.startObject()
                                .field("begin", fsDocumentLocation.getBegin())
                                .field("end", fsDocumentLocation.getEnd())
                                .field("distance", distance)
                                .endObject();
                    }
                }

                builder.endArray();

                Integer first = distances.iterator().next();
                if (first != null) {
                    Set<FsDocumentLocation> fsDocumentLocations = locationAtDistance.get(first);
                    if (fsDocumentLocations != null) {
                        FsDocumentLocation firstLocation = fsDocumentLocations.iterator().next();
                        if (firstLocation != null) {
                            builder.startObject("primaryLocation")
                                    .field("begin", firstLocation.getBegin())
                                    .field("end", firstLocation.getEnd())
                                    .endObject();
                        }
                    }
                }

                builder.endObject();

                UpdateRequestBuilder updateRequest = client
                        .prepareUpdate(systemIndex, "FeatureStructure", key)
                        .setDoc(builder);

                bulkRequestBuilder.add(updateRequest);
            }
        }

        if (bulkRequestBuilder.numberOfActions() > 0) {
            bulkRequestBuilder.get();
        }
    }
}
