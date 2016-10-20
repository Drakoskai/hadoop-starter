package com.drakos.hadoop;

import java.io.File;

/**
 *
 * @author Bruce Brown
 */
public enum HadoopTestUtils {
    ;   
        
    public static final String FSEP = File.separator;
    public static final String TEST_BASE_DIR = "target";
    public static final String HADOOP_RESOURCE_DIR = TEST_BASE_DIR + FSEP + "test-classes";
    public static final String HADOOP_NATIVE_DIR = HADOOP_RESOURCE_DIR + FSEP + "bin";
    public static final String HADOOP_INPUT_DIR = HADOOP_RESOURCE_DIR + FSEP + "input" + FSEP;
    public static final String HADOOP_OUTPUT_DIR = HADOOP_RESOURCE_DIR + FSEP + "output" + FSEP;
    public static final String HADOOP_TEMP_DIR = HADOOP_RESOURCE_DIR + FSEP + "tmp";
}
