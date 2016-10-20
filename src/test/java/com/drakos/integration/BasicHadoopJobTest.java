package com.drakos.integration;

import static com.drakos.hadoop.HadoopTestUtils.HADOOP_INPUT_DIR;
import static com.drakos.hadoop.HadoopTestUtils.HADOOP_OUTPUT_DIR;
import com.drakos.hadoop.MiniHadoop;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Bruce Brown
 */
public class BasicHadoopJobTest {

    private static MiniHadoop miniHadoop;

    @BeforeClass
    public static void setUpClass() {
        miniHadoop = new MiniHadoop(2, 2);
    }

    @AfterClass
    public static void tearDownClass() {
        miniHadoop.close();
    }

    @Test
    public void test() throws Exception {
        String[] args = new String[]{HADOOP_INPUT_DIR + "file01.txt", HADOOP_OUTPUT_DIR};
        int exitCode = ToolRunner.run(new com.drakos.WordCount(), args);
        assert (exitCode == 0);
    }
}
