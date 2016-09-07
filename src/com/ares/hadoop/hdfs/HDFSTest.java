package com.ares.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HDFSTest {
	private static final Logger LOGGER = Logger.getLogger(HDFSTest.class);
	
	private FileSystem fs = null;
	
	@Before
	public void setUp() throws IOException, URISyntaxException, InterruptedException {
		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://HADOOP-NODE1:9000");
//		URI uri = new URI("hdfs://HADOOP-NODE1:9000");
//		this.fs = FileSystem.get(uri, conf, "HADOOP");
		this.fs = FileSystem.get(conf);
	}
	@After
	public void tearDown() throws IOException {
		// TODO Auto-generated method stub
		this.fs.close();
	}
	
	@Test
	public void testGET() throws IOException {
		// TODO Auto-generated method stub 
		LOGGER.debug("HDFSTest: GET FILE TEST");
		Path path = new Path("hdfs://HADOOP-NODE1:9000/jdk-7u60-linux-x64.tar.gz");
		FSDataInputStream fsDataInputStream = this.fs.open(path);
		FileOutputStream fileOutputStream = new FileOutputStream("./testdata/get-test-jdk.tar.gz");
		IOUtils.copy(fsDataInputStream, fileOutputStream);
	}
	
	@Test
	public void testPUT() throws IOException {
		// TODO Auto-generated method stub 
		LOGGER.debug("HDFSTest: PUT FILE TEST");
		Path path = new Path("hdfs://HADOOP-NODE1:9000/put-test-jdk.tar.gz");
		FSDataOutputStream fsDataOutputStream = this.fs.create(path);
		FileInputStream fileInputStream = new FileInputStream("./testdata/test-jdk.tar.gz");
		IOUtils.copy(fileInputStream, fsDataOutputStream);
	}
	
	@Test
	public void testGET_NEW() throws IOException {
		// TODO Auto-generated method stub 
		LOGGER.debug("HDFSTest: GET_NEW FILE TEST");
		Path src = new Path("hdfs://HADOOP-NODE1:9000/jdk-7u60-linux-x64.tar.gz");
		Path dst = new Path("./testdata/get-test-new-jdk.tar.gz");
		this.fs.copyToLocalFile(src, dst);
	}
	
	@Test
	public void testPUT_NEW() throws IOException {
		// TODO Auto-generated method stub 
		LOGGER.debug("HDFSTest: PUT_NEW FILE TEST");
		Path src = new Path("./testdata/test-jdk.tar.gz");
		Path dst = new Path("hdfs://HADOOP-NODE1:9000/put-test-new-jdk.tar.gz");
		this.fs.copyFromLocalFile(src , dst);
	}
	
	@Test
	public void testMKDIR() throws IOException {
		// TODO Auto-generated method stub 
		LOGGER.debug("HDFSTest: MKDIR TEST");
		Path f = new Path("/mkdir-test/testa/testb");
		this.fs.mkdirs(f);
	}
	
	@Test
	public void testRM() throws IOException {
		// TODO Auto-generated method stub 
		LOGGER.debug("HDFSTest: RM TEST");
		Path f = new Path("/mkdir-test");
		this.fs.delete(f, true);
	}
	
	@Test
	public void testLIST() throws IOException {
		// TODO Auto-generated method stub 
		LOGGER.debug("HDFSTest: LIST TEST");
		Path f = new Path("/");
		//LIST FILES
		RemoteIterator<LocatedFileStatus> files = this.fs.listFiles(f, true);
		while (files.hasNext()) {
			LocatedFileStatus file = files.next();
			LOGGER.debug(file.getPath());
			LOGGER.debug(file.getPath().getName());
		}
		
		//LIST DIRS
		FileStatus[] files2 = this.fs.listStatus(f);
//		for (int i = 0; i < files2.length; i++) {
//			LOGGER.debug(files2[i].getPath().getName());
//		}
		for (FileStatus fileStatus : files2) {
			LOGGER.debug(fileStatus.getPath().getName());
			LOGGER.debug(fileStatus.isDirectory());
		}
	}
	
	@Test
	public void testSplit() {
		String text = "1363157985066 \t13726230503\t00-FD-07-A4-72-B8:CMCC\t120.196.100.82\ti02.c.aliimg.com\t\t24\t27\t2481\t24681\t200\n\n";
		String[] fields = StringUtils.split(text, '\t');
		LOGGER.debug(fields.length);
		LOGGER.debug(fields);
		for (int i = 0; i < fields.length; i++) {
			LOGGER.debug("FIELD: " + fields[i]);
		}
	}
}
