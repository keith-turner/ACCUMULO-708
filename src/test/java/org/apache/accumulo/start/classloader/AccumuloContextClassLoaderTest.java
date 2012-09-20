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

public class AccumuloContextClassLoaderTest {
  private static final String HDFS_URI = "hdfs://localhost:8020";
  private static final Path TEST_DIR = new Path(HDFS_URI + "/test-dir");
  private static final Path TEST_DIR2 = new Path(HDFS_URI + "/test-dir2");

  private Configuration conf = null;
  private FileSystem hdfs = null;
  private DefaultFileSystemManager vfs = null;
  private AccumuloContextClassLoader cl = null;
  
  @Before
  public void setup() throws Exception {
    //Setup HDFS
    conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, HDFS_URI);
    this.hdfs = FileSystem.get(conf);
    this.hdfs.mkdirs(TEST_DIR);
    this.hdfs.mkdirs(TEST_DIR2);
    
    //Copy jar file to TEST_DIR
    URL jarPath = this.getClass().getResource("/HelloWorld.jar");
    Path src = new Path(jarPath.toURI().toString());
    Path dst = new Path(TEST_DIR, src.getName());
    this.hdfs.copyFromLocalFile(src, dst);

    Path dst2 = new Path(TEST_DIR2, src.getName());
    this.hdfs.copyFromLocalFile(src, dst2);

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
  public void differentContexts() throws Exception {
    FileObject testDir = vfs.resolveFile(TEST_DIR.toUri().toString());
    FileObject[] dirContents = testDir.getChildren();
    cl = new AccumuloContextClassLoader(dirContents, vfs, ClassLoader.getSystemClassLoader());
    FileObject[] files = cl.getClassLoader(AccumuloContextClassLoader.DEFAULT_CONTEXT).getFiles();
    Assert.assertArrayEquals(dirContents, files);

    FileObject testDir2 = vfs.resolveFile(TEST_DIR2.toUri().toString());
    FileObject[] dirContents2 = testDir2.getChildren();
    cl.addContext("MYCONTEXT", dirContents2);
    FileObject[] files2 = cl.getClassLoader("MYCONTEXT").getFiles();
    Assert.assertArrayEquals(dirContents2, files2);
    
    Class<?> defaultContextClass = cl.loadClass("test.HelloWorld");
    Object o1 = defaultContextClass.newInstance();
    Assert.assertEquals("Hello World!", o1.toString());

    Class<?> myContextClass = cl.loadClass("MYCONTEXT", "test.HelloWorld");
    Object o2 = myContextClass.newInstance();
    Assert.assertEquals("Hello World!", o2.toString());
    
    Assert.assertFalse(defaultContextClass.equals(myContextClass));

  }
  
  @After
  public void destroy() throws Exception {
    cl.close();
    this.hdfs.delete(TEST_DIR, true);
    this.hdfs.close();
    this.vfs.close();
  }

}
