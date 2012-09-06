package org.apache.commons.vfs2.provider;

import java.io.IOException;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReadOnlyHdfsFileProviderTest {
  
  private static final String HDFS_URI = "hdfs://localhost:8020";
  private static final String TEST_FILE1 = HDFS_URI + "/accumulo-test-1.jar";
  private static final Path FILE1_PATH = new Path("/accumulo-test-1.jar");
  
  private DefaultFileSystemManager manager = null;
  
  
  @Before
  public void setup() throws Exception {
    manager = new DefaultFileSystemManager();
    manager.addProvider("hdfs", new ReadOnlyHdfsFileProvider());
    manager.init();
  }
  
  private FileSystem getHDFS() throws IOException {
    Configuration conf = new Configuration();
    conf.set(org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY, HDFS_URI);
    FileSystem fs = FileSystem.get(conf);
    return fs;
  }
  
  @Test
  public void testInit() throws Exception {
    FileObject fo = manager.resolveFile(TEST_FILE1);
    Assert.assertNotNull(fo);
  }
  
  @Test
  public void testExistsFails() throws Exception {
    FileObject fo = manager.resolveFile(TEST_FILE1);
    Assert.assertNotNull(fo);
    Assert.assertFalse(fo.exists());
  }
  
  @Test
  public void testExistsSucceeds() throws Exception {
    FileObject fo = manager.resolveFile(TEST_FILE1);
    Assert.assertNotNull(fo);
    Assert.assertFalse(fo.exists());
    
    //Create the file
    FileSystem hdfs = getHDFS();
    hdfs.create(FILE1_PATH).close();
    Assert.assertTrue(fo.exists());
    
    hdfs.delete(FILE1_PATH, false);
    hdfs.close();
    Assert.assertFalse(fo.exists());
    
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testCanRenameTo() throws Exception {
    FileObject fo = manager.resolveFile(TEST_FILE1);
    Assert.assertNotNull(fo);
    fo.canRenameTo(fo);
  }
  
  @After
  public void destroy() {
    manager.close();
  }
  
}
