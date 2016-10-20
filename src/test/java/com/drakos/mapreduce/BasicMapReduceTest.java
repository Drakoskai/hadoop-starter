package com.drakos.mapreduce;

import static com.drakos.hadoop.HadoopTestUtils.HADOOP_INPUT_DIR;
import static com.drakos.hadoop.HadoopTestUtils.HADOOP_RESOURCE_DIR;
import static com.drakos.hadoop.HadoopTestUtils.HADOOP_TEMP_DIR;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Bruce Brown
 */
public class BasicMapReduceTest {

    private MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduce;
    private Text testDoc;

    @BeforeClass
    public static void setupClass() throws IOException {
        File homeDir = new File(HADOOP_RESOURCE_DIR);
        File tmpDir = new File(HADOOP_TEMP_DIR);

        if (tmpDir.exists()) {
            FileUtils.forceDelete(tmpDir);
        }

        FileUtils.forceMkdir(tmpDir);
        System.setProperty("hadoop.home.dir", homeDir.getAbsolutePath());
        System.setProperty("hadoop.tmp.dir", tmpDir.getAbsolutePath());
        System.setProperty("hadoop.log.dir", new File(tmpDir, "logs").getAbsolutePath());
    }

    @Before
    public void setup() throws IOException {
        testDoc = new Text(new String(Files.readAllBytes(Paths.get(HADOOP_INPUT_DIR + "file01.txt"))));
        TokenizerMapper mapper = new TokenizerMapper();
        IntSumReducer reducer = new IntSumReducer();
        mapReduce = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapperReduce() throws IOException {
        mapReduce.withInput(NullWritable.get(), new Text(testDoc));
        List<Pair<Text, IntWritable>> result = mapReduce.run();
        result.stream().forEach(System.out::println);
    }
}
