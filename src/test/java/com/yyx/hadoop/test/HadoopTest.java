package com.yyx.hadoop.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;


public class HadoopTest {

    static String HADOOP_PATH = "hdfs://hadoop0:8020";
    Configuration conf = null;

    static FileSystem fs = null;
    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        fs = FileSystem.get(URI.create(HADOOP_PATH), conf, "hadoop");

    }


    @Test

    public void TestGetPathFromHdfs(){
        System.out.println(getPathFromHdfs(HADOOP_PATH,"hdfs://hadoop0:8020/a.txt is file"));
    }


    public String getPathFromHdfs(String uriPath,String hdfsPath){

        int uriLength = uriPath.length();

        String path = hdfsPath.substring(uriLength);

        return path;
    }

    /***
     * 递归列出目录下的文件信息
     * @throws Exception
     */


    @Test
    public void testListDir() throws Exception {
        listDir("/");
    }


    public void listDir(String path) throws Exception {

        //FileStatus fileStatus = fs.getFileStatus(new Path("/"));
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));

        for (FileStatus fileStatuse : fileStatuses) {
            if (fileStatuse.isDirectory()) {
                System.out.println(fileStatuse.getPath().toString()+"--dir");
                listDir(getPathFromHdfs(HADOOP_PATH, fileStatuse.getPath().toString()));
            }
            else {
                System.out.println(fileStatuse.getPath().toString() + "--file");
            }
        }
    }

}
