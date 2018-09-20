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
package com.msd.gin.halyard.common;

import java.io.File;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HBaseServerTestInstance {

    private static Configuration conf = null;

    public static synchronized Configuration getInstanceConfig() throws Exception {
        if (conf == null) {
            File zooRoot = File.createTempFile("hbase-zookeeper", "");
            zooRoot.delete();
            ZooKeeperServer zookeper = new ZooKeeperServer(zooRoot, zooRoot, 2000);
            ServerCnxnFactory factory = ServerCnxnFactory.createFactory(new InetSocketAddress("localhost", 0), 5000);
            factory.startup(zookeper);

            YarnConfiguration yconf = new YarnConfiguration();
            String argLine = System.getProperty("argLine");
            if (argLine != null) {
                yconf.set("yarn.app.mapreduce.am.command-opts", argLine.replace("jacoco.exec", "jacocoMR.exec"));
            }
            yconf.setBoolean(MRConfig.MAPREDUCE_MINICLUSTER_CONTROL_RESOURCE_MONITORING, false);
            yconf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
            MiniMRYarnCluster miniCluster = new MiniMRYarnCluster("testCluster");
            miniCluster.init(yconf);
            String resourceManagerLink = yconf.get(YarnConfiguration.RM_ADDRESS);
            yconf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, true);
            miniCluster.start();
            miniCluster.waitForNodeManagersToConnect(10000);
            // following condition set in MiniYarnCluster:273
            while (resourceManagerLink.endsWith(":0")) {
                Thread.sleep(100);
                resourceManagerLink = yconf.get(YarnConfiguration.RM_ADDRESS);
            }

            File hbaseRoot = File.createTempFile("hbase-root", "");
            hbaseRoot.delete();
            conf = HBaseConfiguration.create(miniCluster.getConfig());
            conf.set(HConstants.HBASE_DIR, hbaseRoot.toURI().toURL().toString());
            conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, factory.getLocalPort());
            conf.set("hbase.master.hostname", "localhost");
            conf.set("hbase.regionserver.hostname", "localhost");
            conf.setInt("hbase.master.info.port", -1);
            conf.set("hbase.fs.tmp.dir", new File(System.getProperty("java.io.tmpdir")).toURI().toURL().toString());
            LocalHBaseCluster cluster = new LocalHBaseCluster(conf);
            cluster.startup();
        }
        return new Configuration(conf);
    }
}
