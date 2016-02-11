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

import com.google.common.io.ByteStreams;
import edu.umn.nlptab.NlpTabException;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

/**
 * Represents a mapping for a type in the ElasticSearch server.
 *
 * @see <a href="http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/mapping.html">http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/mapping.html</a>
 */
class EsTypeMapping {
    private static final ESLogger LOGGER = Loggers.getLogger(EsTypeMapping.class);
    private final IndicesAdminClient indicesAdminClient;
    private String index;
    private String type;
    private String mapping;

    /**
     * Default constructor, initializes the ElasticSearch Client where the type mapping will be put
     * @param client ElasticSearch Client
     */
    @Inject
    EsTypeMapping(Client client) {
        indicesAdminClient = client.admin().indices();
    }

    /**
     * Fluent interface which sets the mapping data from a resource path on the classpath, usually of a json file
     * containing the mapping data
     *
     * @param resourcePath a path to load a resource from the classpath
     * @return this EsTypeMapping
     * @throws NlpTabException if the resource fails to read.
     */
    public EsTypeMapping setMappingFromClasspathResource(String resourcePath) throws NlpTabException {
        try {
            InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath);
            mapping = new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new NlpTabException(e);
        }
        return this;
    }

    /**
     * Sets the ElasticSearch type that this mapping is for.
     * @param type ElasticSearch type
     * @return this EsTypeMapping
     */
    public EsTypeMapping setType(String type) {
        this.type = type;
        return this;
    }

    /**
     * Sets the ElasticSearch index that this mapping will be added to.
     * @param index ElasticSearch index
     * @return this EsTypeMapping
     */
    public EsTypeMapping setIndex(String index) {
        this.index = index;
        return this;
    }

    /**
     * Puts the type mapping to the ElasticSearch server specified by the client in the constructor.
     *
     * @throws NlpTabException
     */
    public void putTypeMapping() throws NlpTabException {
        IndicesExistsResponse indicesExistsResponse;
        try {
            indicesExistsResponse = indicesAdminClient.prepareExists(index).execute().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new NlpTabException(e);
        }
        if (!indicesExistsResponse.isExists()) {
            try {
                indicesAdminClient.prepareCreate(index).execute().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new NlpTabException(e);
            }
        }

        PutMappingRequestBuilder putMappingRequestBuilder = indicesAdminClient.preparePutMapping(index);
        putMappingRequestBuilder.setType(type);
        putMappingRequestBuilder.setSource(mapping);

        LOGGER.debug("Adding mapping with type: {} to index: {}", type, index);

        PutMappingResponse putMappingResponse;
        try {
            putMappingResponse = putMappingRequestBuilder.execute().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new NlpTabException(e);
        }
        if (!putMappingResponse.isAcknowledged()) {
            LOGGER.debug("Adding mapping with type: {} to index: {} failed", type, index);
            throw new NlpTabException("Failed to add mapping type");
        }
    }
}
