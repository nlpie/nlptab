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
import org.apache.uima.cas.FeatureStructure;

import java.io.IOException;

/**
 *
 */
public interface CasProcessingDelegate {
    boolean shouldProcessView(String viewName) throws NlpTabException;

    void priorToProcessing(SofaData sofaData) throws NlpTabException;

    void willProcess(FeatureStructure featureStructure, TypeSystemInfo typeSystemInfo) throws NlpTabException;

    void afterProcessing(SofaData sofaData) throws InterruptedException, IOException;
}
