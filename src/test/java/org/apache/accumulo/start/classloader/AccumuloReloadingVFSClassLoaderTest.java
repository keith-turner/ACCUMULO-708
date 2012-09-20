package org.apache.accumulo.start.classloader;

import java.net.URL;

import org.apache.commons.vfs2.CacheStrategy;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.cache.DefaultFilesCache;
import org.apache.commons.vfs2.cache.SoftRefFilesCache;
import org.apache.commons.vfs2.impl.DefaultFileReplicator;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.impl.FileContentInfoFilenameFactory;
import org.apache.commons.vfs2.provider.ReadOnlyHdfsFileProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AccumuloReloadingVFSClassLoaderTest {
 
  private static final String HDFS_URI = "hdfs://localhost:8020";
  private static final Path TEST_DIR = new Path(HDFS_URI + "/test-dir");

  private Configuration conf = null;
  private FileSystem hdfs = null;
  private DefaultFileSystemManager vfs = null;
  private AccumuloReloadingVFSClassLoader cl = null;
 
  @Before
  public void setup() throws Exception {
    //Setup HDFS
    conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, HDFS_URI);
    this.hdfs = FileSystem.get(conf);
    this.hdfs.mkdirs(TEST_DIR);
    
    //Copy jar file to TEST_DIR
    URL jarPath = this.getClass().getResource("/HelloWorld.jar");
    Path src = new Path(jarPath.toURI().toString());
    Path dst = new Path(TEST_DIR, src.getName());
    this.hdfs.copyFromLocalFile(src, dst);
    
    //Set up the VFS
    vfs = new DefaultFileSystemManager();
    vfs.setFilesCache(new DefaultFilesCache());
    vfs.addProvider("res", new org.apache.commons.vfs2.provider.res.ResourceFileProvider());
    vfs.addProvider("zip", new org.apache.commons.vfs2.provider.zip.ZipFileProvider());
    vfs.addProvider("gz", new org.apache.commons.vfs2.provider.gzip.GzipFileProvider());
    vfs.addProvider("ram", new org.apache.commons.vfs2.provider.ram.RamFileProvider());
    vfs.addProvider("file", new org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider());
    vfs.addProvider("jar", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
    vfs.addProvider("hdfs", new ReadOnlyHdfsFileProvider());
    vfs.addExtensionMap("jar", "jar");
    vfs.setFileContentInfoFactory(new FileContentInfoFilenameFactory());
    vfs.setFilesCache(new SoftRefFilesCache());
    vfs.setReplicator(new DefaultFileReplicator());
    vfs.setCacheStrategy(CacheStrategy.ON_RESOLVE);
    vfs.init();
  }
  
  @Test
  public void testConstructor() throws Exception {
    FileObject testDir = vfs.resolveFile(TEST_DIR.toUri().toString());
    FileObject[] dirContents = testDir.getChildren();
    cl = new AccumuloReloadingVFSClassLoader(dirContents, vfs, ClassLoader.getSystemClassLoader());
    FileObject[] files = cl.getFiles();
    Assert.assertArrayEquals(dirContents, files);
  }
  
  @Test
  public void testReloading() throws Exception {
    FileObject testDir = vfs.resolveFile(TEST_DIR.toUri().toString());
    FileObject[] dirContents = testDir.getChildren();
    cl = new AccumuloReloadingVFSClassLoader(dirContents, vfs, ClassLoader.getSystemClassLoader());
    FileObject[] files = cl.getFiles();
    Assert.assertArrayEquals(dirContents, files);

    Class<?> clazz1 = cl.loadClass("test.HelloWorld");
    Object o1 = clazz1.newInstance();
    Assert.assertEquals("Hello World!", o1.toString());

    //Check that the class is the same before the update
    Class<?> clazz1_5 = cl.loadClass("test.HelloWorld");
    Assert.assertEquals(clazz1, clazz1_5);
    
    //Update the class
    URL jarPath = this.getClass().getResource("/HelloWorld.jar");
    Path src = new Path(jarPath.toURI().toString());
    Path dst = new Path(TEST_DIR, "HelloWorld.jar");
    this.hdfs.copyFromLocalFile(src, dst);

    //Wait for the monitor to notice
    Thread.sleep(4000);
    
    Class<?> clazz2 = cl.loadClass("test.HelloWorld");
    Object o2 = clazz2.newInstance();
    Assert.assertEquals("Hello World!", o2.toString());
    
    //This is false because they are loaded by a different classloader
    Assert.assertFalse(clazz1.equals(clazz2));
    Assert.assertFalse(o1.equals(o2));
    
  }
  
  @After
  public void destroy() throws Exception {
    cl.close();
    this.hdfs.delete(TEST_DIR, true);
    this.hdfs.close();
    this.vfs.close();
  }

}
