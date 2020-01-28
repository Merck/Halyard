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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.mapdb.DB;
import org.mapdb.DBMaker;

/**
 * TODO
 * This is a MapDB implementation, however a hash set backed by HDFS is expected here.
 * Thread-safe.
 * @author Adam Sotona (MSD)
 * @param <E> Serializable element type
 */
public class BigHashSet<E extends Serializable> implements Iterable<E>, Closeable {

    private static final String SET_NAME = "temp";

    private Set<E> set;
    private DB db;

    public static <E extends Serializable> BigHashSet<E> create() {
    	return new BigHashSet<>();
    }

    private BigHashSet() {
    	this.set = new HashSet<>(1024);
    }

    /**
     * Adds element to the BigHashSet
     * @param e Serializable element
     * @return boolean if the element has been already present
     * @throws IOException throws IOException in case of problem with underlying storage
     */
    public synchronized boolean add(E e) throws IOException {
    	if (set == null) {
    		throw new IOException("Already closed");
    	}

    	if (db == null && set.size() > 100000) {
    		swapToDisk();
    	}

    	try {
            return set.add(e);
        } catch (IllegalAccessError err) {
            throw new IOException(err);
        }
    }

    private synchronized void swapToDisk() {
    	if (db != null) {
    		return;
    	}

    	db = DBMaker.newTempFileDB().deleteFilesAfterClose().closeOnJvmShutdown().transactionDisable().asyncWriteEnable().make();
        Set<E> dbSet = db.getHashSet(SET_NAME);
        dbSet.addAll(set);
        set = dbSet;
    }

    @Override
    public synchronized Iterator<E> iterator() {
        return set.iterator();
    }

    /**
     * Checks for element presence in the BigHashSet
     * @param e Serializable element
     * @return boolean if the element has been present
     * @throws IOException throws IOException in case of problem with underlying storage
     */
    public synchronized boolean contains(E e) throws IOException {
    	if (set == null) {
    		throw new IOException("Already closed");
    	}

    	try {
            return set.contains(e);
        } catch (IllegalAccessError err) {
            throw new IOException(err);
        }
    }

    @Override
    public synchronized void close() {
    	set = null;
    	if (db != null) {
	        try {
	            db.close();
	        } catch (IllegalAccessError|IllegalStateException ignore) {
	            //silent close
	        } finally {
	        	db = null;
	        }
    	}
    }
}
