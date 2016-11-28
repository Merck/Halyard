/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.strategy.collections;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;
import org.mapdb.DB;
import org.mapdb.DBMaker;

/**
 * TODO
 * This is a MapDB implementation, however a hash set backed by HDFS is expected here
 * @author Adam Sotona (MSD)
 * @param <E> Serializable element type
 */
public class BigHashSet<E extends Serializable> implements Iterable<E>, Closeable {

    private static final String SET_NAME = "temp";

    private final DB db = DBMaker.newTempFileDB().deleteFilesAfterClose().closeOnJvmShutdown().transactionDisable().make();
    private final Set<E> set = db.getHashSet(SET_NAME);

    /**
     * Adds element to the BigHashSet
     * @param e Serializable element
     * @return boolean if the element has been already present
     * @throws IOException throws IOException in case of problem with underlying storage
     */
    public boolean add(E e) throws IOException {
        try {
            return set.add(e);
        } catch (IllegalAccessError err) {
            throw new IOException(err);
        }
    }

    @Override
    public Iterator<E> iterator() {
        return set.iterator();
    }

    /**
     * Checks for element presence in the BigHashSet
     * @param e Serializable element
     * @return boolean if the element has been present
     * @throws IOException throws IOException in case of problem with underlying storage
     */
    public boolean contains(E e) throws IOException {
        try {
            return set.contains(e);
        } catch (IllegalAccessError err) {
            throw new IOException(err);
        }
    }

    @Override
    public void close() {
        try {
            db.close();
        } catch (IllegalAccessError ignore) {
            //silent close
        }
    }
}
