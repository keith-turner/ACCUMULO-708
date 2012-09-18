package org.apache.commons.vfs2.provider;

import java.net.URL;

import org.apache.commons.vfs2.CacheStrategy;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.cache.DefaultFilesCache;
import org.apache.commons.vfs2.cache.SoftRefFilesCache;
import org.apache.commons.vfs2.impl.DefaultFileReplicator;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.impl.FileContentInfoFilenameFactory;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class VfsClassLoaderTest {
  
  private static final String HDFS_URI = "hdfs://localhost:8020";
  private static final Path TEST_DIR = new Path(HDFS_URI + "/test-dir");

  private Configuration conf = null;
  private FileSystem hdfs = null;
  private DefaultFileSystemManager vfs = null;
  private VFSClassLoader cl = null;
  
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
    
    FileObject testDir = vfs.resolveFile(TEST_DIR.toUri().toString());
    FileObject[] dirContents = testDir.getChildren();
//for (FileObject f : dirContents)
//  System.out.println(f.getURL().toString());
    //Point the VFSClassLoader to all of the objects in TEST_DIR
    this.cl = new VFSClassLoader(dirContents, vfs);
  }

  @Test
  public void testGetClass() throws Exception {
    Class<?> helloWorldClass = this.cl.loadClass("test.HelloWorld");
    Object o = helloWorldClass.newInstance();
    Assert.assertEquals("Hello World!", o.toString());
  }
  
  @After
  public void destroy() throws Exception {
    this.hdfs.delete(TEST_DIR, true);
    this.hdfs.close();
    this.vfs.close();
  }
  
}
