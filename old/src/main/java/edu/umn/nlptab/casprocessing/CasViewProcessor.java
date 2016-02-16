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

import edu.umn.nlptab.NlpTabException;
import edu.umn.nlptab.uimatyping.TypeSystemInfo;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.impl.LowLevelCAS;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;


public class CasViewProcessor {
    private static final int POLL_FS_QUEUE_EVERY_MS = 100;

    private static final ESLogger LOGGER = Loggers.getLogger(CasViewProcessor.class);

    private final Client client;

    private final BlockingQueue<Integer> fsRefQueue;

    private final FeatureStructureProcessorFactory featureStructureProcessorFactory;

    private final CasProcessorSettings casProcessorSettings;

    private final TypeSystemInfo typeSystemInfo;

    private final CasProcessingDelegate casProcessingDelegate;

    private final SofaData sofaData;

    private final LowLevelCAS lowLevelCAS;

    @Inject
    CasViewProcessor(Client client,
                     FeatureStructureProcessorFactory featureStructureProcessorFactory,
                     @Assisted CasProcessorSettings casProcessorSettings,
                     @Assisted SofaData sofaData) throws InterruptedException {
        this.client = client;
        this.featureStructureProcessorFactory = featureStructureProcessorFactory;

        this.casProcessorSettings = casProcessorSettings;
        typeSystemInfo = casProcessorSettings.getTypeSystemInfo();
        casProcessingDelegate = casProcessorSettings.getCasProcessingDelegate();


        this.sofaData = sofaData;
        this.fsRefQueue = sofaData.getFsRefQueue();

        CAS cas = sofaData.getCas();

        lowLevelCAS = cas.getLowLevelCAS();
        Type topType = cas.getTypeSystem().getTopType();
        FSIterator<FeatureStructure> allIndexedFS = cas.getIndexRepository().getAllIndexedFS(topType);
        while (allIndexedFS.hasNext()) {
            sofaData.getIdentifierForFs(allIndexedFS.next());
        }
    }

    void process() throws NlpTabException {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        casProcessingDelegate.priorToProcessing(sofaData);
        int processed = 0;
        try {
            while (!fsRefQueue.isEmpty()) {
                Integer fsRef = fsRefQueue.poll(POLL_FS_QUEUE_EVERY_MS, TimeUnit.MILLISECONDS);
                if (fsRef == null) {
                    continue;
                }
                FeatureStructure featureStructure = lowLevelCAS.ll_getFSForRef(fsRef);

                casProcessingDelegate.willProcess(featureStructure, typeSystemInfo);

                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Processing feature structure with type {}", featureStructure.getType().getName());
                }

                IndexRequestBuilder indexRequestBuilder = featureStructureProcessorFactory
                        .create(casProcessorSettings, sofaData, featureStructure)
                        .process();

                if (indexRequestBuilder != null) {
                    bulkRequestBuilder.add(indexRequestBuilder);
                }
                processed++;
            }
            if (bulkRequestBuilder.numberOfActions() > 0) {
                bulkRequestBuilder.get();
            }
            casProcessingDelegate.afterProcessing(sofaData);
        } catch (InterruptedException | IOException e) {
            throw new NlpTabException("Interrupted while attempting to upload search sofa", e);
        }
        LOGGER.debug("Number of FeatureStructures processed: {}", processed);
    }
}
