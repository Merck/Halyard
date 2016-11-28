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
import java.util.Map;
import java.util.NavigableMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

/**
 * Sorter does not preserve unique instances. All equal instances are mapped to a single instance.
 * TODO
 * This is a MapDB implementation, however a merge-sort backed by HDFS is expected here.
 * @author Adam Sotona (MSD)
 * @param <E> Comparable and Serializable element type
 */
public class Sorter <E extends Comparable<E> & Serializable> implements Iterable<Map.Entry<E, Long>>, Closeable {

    private static final String MAP_NAME = "temp";

    private final DB db;
    private final NavigableMap<E, Long> map;
    private final long limit;
    private final boolean distinct;
    private long size = 0;

    /**
     * Constructs Sorter with optional limit and optional distinct filtering
     * @param limit long limit, where Long.MAXLONG means no limit
     * @param distinct optional boolean switch to do not preserve multiple equal elements
     */
    public Sorter(long limit, boolean distinct) {
        this.db = DBMaker.newTempFileDB().deleteFilesAfterClose().closeOnJvmShutdown().transactionDisable().make();
        this.map = db.createTreeMap(MAP_NAME).make();
        this.limit = limit;
        this.distinct = distinct;
    }

    /**
     * Adds new element to the sorter
     * @param e element to be added to Sorter
     * @throws IOException throws IOException in case of problem with underlying storage
     */
    public void add(E e) throws IOException {
        if (size < limit || e.compareTo(map.lastKey()) < 0) try {
            Long c = map.get(e);
            if (c == null) {
                map.put(e, 1l);
                size ++;
            } else if (!distinct) {
                map.put(e, c + 1l);
                size ++;
            }
            while (size > limit) {
                // Discard key(s) that are currently sorted last
                Map.Entry<E, Long> last = map.lastEntry();
                if (last.getValue() > size - limit) {
                    map.put(last.getKey(), last.getValue() + limit - size);
                    size = limit;
                } else {
                    map.remove(last.getKey());
                    size -= last.getValue();
                }
            }
        } catch (IllegalAccessError err) {
            throw new IOException(err);
        }
    }

    @Override
    public Iterator<Map.Entry<E, Long>> iterator() {
        return map.entrySet().iterator();
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
