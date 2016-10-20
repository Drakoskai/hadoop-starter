package com.drakos.hadoop;

import static com.drakos.hadoop.HadoopTestUtils.FSEP;
import static com.drakos.hadoop.HadoopTestUtils.HADOOP_NATIVE_DIR;
import static com.drakos.hadoop.HadoopTestUtils.HADOOP_OUTPUT_DIR;
import static com.drakos.hadoop.HadoopTestUtils.HADOOP_RESOURCE_DIR;
import static com.drakos.hadoop.HadoopTestUtils.TEST_BASE_DIR;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;

/**
 *
 * @author Bruce Brown
 */
public class MiniHadoop {

    private MiniDFSCluster dfsCluster;
    private MiniMRClientCluster mrCluster;
    private FileSystem fileSystem;

    public MiniHadoop(final int taskTrackers, final int dataNodes) {
        this(getConf(), taskTrackers, dataNodes);
    }

    public MiniHadoop(final Configuration config, final int taskTrackers, final int dataNodes) {
        if (taskTrackers < 1) {
            throw new IllegalArgumentException("Invalid taskTrackers value, must be greater than 0");
        }
        if (dataNodes < 1) {
            throw new IllegalArgumentException("Invalid dataNodes value, must be greater than 0");
        }

        File homeDir = new File(HADOOP_RESOURCE_DIR);
        File tmpDir = new File(TEST_BASE_DIR + FSEP + "tmp");
        File outDir = new File(HADOOP_OUTPUT_DIR);

        config.set("hadoop.home.dir", homeDir.getAbsolutePath());
        config.set("hadoop.tmp.dir", tmpDir.getAbsolutePath());
        config.set(MiniDFSCluster.PROP_TEST_BUILD_DATA, homeDir.getAbsolutePath());
        config.set("hadoop.log.dir", new File(tmpDir, "logs").getAbsolutePath());
        config.set("HADOOP_OPTS", "-Djava.library.path=" + new File(HADOOP_NATIVE_DIR).getAbsolutePath());
        config.set("java.library.path", new File(HADOOP_NATIVE_DIR).getAbsolutePath());
        try {
            if (tmpDir.exists()) {
                FileUtils.forceDelete(tmpDir);
            }

            if (outDir.exists()) {
                FileUtils.forceDelete(outDir);
            }

            FileUtils.forceMkdir(tmpDir);
            System.setProperty("hadoop.home.dir", homeDir.getAbsolutePath());
            System.setProperty("hadoop.tmp.dir", tmpDir.getAbsolutePath());
            System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, new File(tmpDir, "data").getAbsolutePath());
            System.setProperty("hadoop.log.dir", new File(tmpDir, "logs").getAbsolutePath());
            System.setProperty("HADOOP_OPTS", "-Djava.library.path=" + new File(HADOOP_NATIVE_DIR).getAbsolutePath());
            System.setProperty("java.library.path", new File(HADOOP_NATIVE_DIR).getAbsolutePath());

            dfsCluster = new MiniDFSCluster.Builder(config)
                    .numDataNodes(dataNodes)
                    .format(true)
                    .build();

            fileSystem = dfsCluster.getFileSystem();

            mrCluster = MiniMRClientClusterFactory.create(MiniHadoop.class, "test-classes/tmp/mapreduce", taskTrackers, config);
            mrCluster.start();
        } catch (IOException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    public static Configuration getConf() {
        return getConf(new Configuration());
    }

    public static Configuration getConf(Configuration config) {
        File confDir = new File(HADOOP_RESOURCE_DIR, "etc");

        File coreSite = new File(confDir, "core-site.xml");
        if (coreSite.exists()) {
            config.addResource(coreSite.getAbsolutePath());
        }

        File hdfsSite = new File(confDir, "hdfs-site.xml");
        if (coreSite.exists()) {
            config.addResource(hdfsSite.getAbsolutePath());
        }

        File mapredSite = new File(confDir, "mapred-site.xml");
        if (coreSite.exists()) {
            config.addResource(mapredSite.getAbsolutePath());
        }
        File yarnSite = new File(confDir, "yarn-site.xml");
        if (coreSite.exists()) {
            config.addResource(yarnSite.getAbsolutePath());
        }
        return config;
    }

    public void close() {
        if (mrCluster != null) {
            try {
                mrCluster.stop();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        if (dfsCluster != null) {
            try {
                dfsCluster.shutdown();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }
}
