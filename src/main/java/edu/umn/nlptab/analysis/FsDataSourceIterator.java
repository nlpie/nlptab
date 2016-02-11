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

package edu.umn.nlptab.analysis;

import org.elasticsearch.search.SearchHit;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *
 */
class FsDataSourceIterator implements Iterator<SearchHit> {
    private final FsDataSource fsDataSource;
    private int index = 0;
    private int total = 0;

    public FsDataSourceIterator(FsDataSource fsDataSource) {
        this.fsDataSource = fsDataSource;
    }

    @Override
    public boolean hasNext() {
        if (index != total) {
            return true;
        }
        index = 0;
        total = fsDataSource.advance();
        return total > 0;
    }

    @Override
    public SearchHit next() {
        if (index == total) {
            throw new NoSuchElementException();
        }
        return fsDataSource.getSearchHit(index++);
    }
}
