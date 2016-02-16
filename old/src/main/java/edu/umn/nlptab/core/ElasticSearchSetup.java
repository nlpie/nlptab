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

package edu.umn.nlptab.core;

import com.google.common.base.Preconditions;
import edu.umn.nlptab.NlpTabException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 *
 */
public class ElasticSearchSetup {
    private final IndexInitializer indexInitializer;
    private final Provider<EsTypeMapping> typeMappingProvider;

    @Nullable
    private InstanceIndexes instanceIndexes;

    @Nullable
    private String index;

    @Inject
    public ElasticSearchSetup(IndexInitializer indexInitializer,
                              Provider<EsTypeMapping> typeMappingProvider) {
        this.indexInitializer = indexInitializer;
        this.typeMappingProvider = typeMappingProvider;
    }

    public void setInstance(String instance) {
        this.instanceIndexes = InstanceIndexes.of(instance);
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public void setUpPrimaryIndex(String settingsClasspathResourceName) throws NlpTabException {
        Preconditions.checkNotNull(index);

        indexInitializer.setIndex(index);
        try {
            indexInitializer.setSettingsFromClasspathResource(settingsClasspathResourceName);
        } catch (IOException e) {
            throw new NlpTabException(e);
        }
        indexInitializer.initializeIndex();
    }

    private void setUpEsType(String index, String typeName, String classpathResource) throws NlpTabException {
        Preconditions.checkNotNull(index);

        typeMappingProvider.get()
                .setIndex(index)
                .setType(typeName)
                .setMappingFromClasspathResource(classpathResource)
                .putTypeMapping();
    }

    public void setUpPrimaryIndexType(String typeName, String classpathResourceName) throws NlpTabException {
        Preconditions.checkNotNull(index);
        setUpEsType(index, typeName, classpathResourceName);
    }

    public void setUpSearchIndexType(String typeName, String classpathResourceName) throws NlpTabException {
        Preconditions.checkNotNull(instanceIndexes);
        setUpEsType(instanceIndexes.searchIndex(), typeName, classpathResourceName);
    }

    public void setUpMetadataIndexType(String typeName, String classpathResourceName) throws NlpTabException {
        Preconditions.checkNotNull(instanceIndexes);
        setUpEsType(instanceIndexes.metadataIndex(), typeName, classpathResourceName);
    }
}
