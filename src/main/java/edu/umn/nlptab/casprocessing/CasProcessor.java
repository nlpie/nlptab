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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;

import java.util.Iterator;

/**
 *
 */
public class CasProcessor {
    private final CasViewProcessorFactory casViewProcessorFactory;

    private final CasProcessorSettings casProcessorSettings;

    private final CAS cas;

    private final TypeSystemInfo typeSystemInfo;

    private final CasProcessingDelegate casProcessingDelegate;

    @Inject
    CasProcessor(CasViewProcessorFactory casViewProcessorFactory,
                 @Assisted CasProcessorSettings casProcessorSettings,
                 @Assisted CAS cas) {
        this.casViewProcessorFactory = casViewProcessorFactory;
        this.casProcessorSettings = casProcessorSettings;
        typeSystemInfo = casProcessorSettings.getTypeSystemInfo();
        casProcessingDelegate = casProcessorSettings.getCasProcessingDelegate();
        this.cas = cas;
    }

    public void process() throws NlpTabException {
        Iterator<CAS> viewIterator = cas.getViewIterator();

        String casIdentifier = Strings.base64UUID();

        while (viewIterator.hasNext()) {
            CAS casView = viewIterator.next();

            String viewName = casView.getViewName();

            if (casProcessingDelegate.shouldProcessView(viewName)) {
                SofaData sofaData = new SofaData(casIdentifier, casView, typeSystemInfo::isTypeAccepted);

                CasViewProcessor casViewProcessor = casViewProcessorFactory.create(casProcessorSettings, sofaData);

                casViewProcessor.process();
            }
        }
    }
}
