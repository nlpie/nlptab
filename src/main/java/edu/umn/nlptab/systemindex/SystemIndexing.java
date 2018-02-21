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

import com.google.common.collect.ImmutableSet;
import edu.umn.nlptab.casprocessing.CasProcessorFactory;
import edu.umn.nlptab.casprocessing.CasProcessorSettings;
import edu.umn.nlptab.NlpTabException;
import edu.umn.nlptab.core.InstanceIndexes;
import edu.umn.nlptab.uimatyping.TypeFilterBuilder;
import edu.umn.nlptab.uimatyping.TypeFilterLists;
import edu.umn.nlptab.uimatyping.TypeSystemInfo;
import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.cas.admin.CASFactory;
import org.apache.uima.cas.admin.CASMgr;
import org.apache.uima.cas.impl.CASImpl;
import org.apache.uima.cas.impl.XCASDeserializer;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 *
 */
public class SystemIndexing {

    private static final ESLogger LOGGER = Loggers.getLogger(SystemIndexing.class);

    static final String[] TYPE_WHITELIST = new String[]{
            CAS.TYPE_NAME_TOP, CAS.TYPE_NAME_ANNOTATION, CAS.TYPE_NAME_FS_ARRAY, CAS.TYPE_NAME_FS_LIST
    };

    private final CasProcessorFactory casProcessorFactory;

    private final SystemIndexCasProcessingDelegate systemIndexCasViewProcessorDelegate;

    private final Provider<SystemIndexFSProcessorDelegate> systemIndexFSProcessorDelegateProvider;

    private final SystemIndexingSettings systemIndexingSettings;

    private final TypeSystemInfo typeSystemInfo;

    private final TypeSystem typeSystem;

    private final ResourceManager resourceManager;

    private final Properties tuningProperties;

    @Inject
    SystemIndexing(CasProcessorFactory casProcessorFactory,
                   SystemIndexCasProcessingDelegate.Factory casViewProcessorDelegateFactory,
                   Provider<SystemIndexFSProcessorDelegate> systemIndexFSProcessorDelegateProvider,
                   @Assisted SystemIndexingSettings systemIndexingSettings,
                   @Assisted TypeSystemDescription typeSystemDescription) throws NlpTabException {
        this.casProcessorFactory = casProcessorFactory;
        this.systemIndexFSProcessorDelegateProvider = systemIndexFSProcessorDelegateProvider;

        this.systemIndexingSettings = systemIndexingSettings;

        tuningProperties = new Properties();
        tuningProperties.setProperty(UIMAFramework.JCAS_CACHE_ENABLED, "false");

        resourceManager = UIMAFramework.newDefaultResourceManager();

        CASMgr casMgr = CASFactory.createCAS(CASImpl.DEFAULT_INITIAL_HEAP_SIZE, false);
        try {
            CasCreationUtils.setupTypeSystem(casMgr, typeSystemDescription);
        } catch (ResourceInitializationException e) {
            throw new NlpTabException(e);
        }
        ((CASImpl) casMgr).commitTypeSystem();

        typeSystem = ((CASImpl) casMgr).getTypeSystem();

        TypeFilterLists typeFilterLists = TypeFilterLists.create(TYPE_WHITELIST, new String[]{});
        ImmutableSet<String> typeFilter = TypeFilterBuilder.newBuilder()
                .withTypeSystem(typeSystem)
                .withTypeFilterLists(typeFilterLists)
                .createTypeFilter();
        typeSystemInfo = new TypeSystemInfo(typeSystem, typeFilter);

        InstanceIndexes instanceIndexes = systemIndexingSettings.getInstanceIndexes();
        String systemIndex = systemIndexingSettings.getIndex();
        systemIndexCasViewProcessorDelegate = casViewProcessorDelegateFactory.create(instanceIndexes, systemIndex);
    }

    void indexDocument(Path documentPath) throws NlpTabException {
        try (InputStream inputStream = Files.newInputStream(documentPath)) {
            CAS cas = CasCreationUtils.createCas(typeSystem, null, null, tuningProperties, resourceManager);

            if (systemIndexingSettings.useXCas()) {
                XCASDeserializer.deserialize(inputStream, cas, true);
            } else {
                XmiCasDeserializer.deserialize(inputStream, cas, true);
            }

            CasProcessorSettings casProcessorSettings = new CasProcessorSettings(systemIndexingSettings.getIndex(),
                    typeSystemInfo, systemIndexFSProcessorDelegateProvider, systemIndexCasViewProcessorDelegate);

            casProcessorFactory.create(casProcessorSettings, cas).process();
        } catch (ResourceInitializationException | IOException | SAXException e) {
            throw new NlpTabException(e);
        }
    }
}
