package org.apache.commons.vfs2.provider;

import java.net.URL;

import org.apache.commons.vfs2.CacheStrategy;
import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.cache.DefaultFilesCache;
import org.apache.commons.vfs2.cache.SoftRefFilesCache;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.apache.commons.vfs2.impl.DefaultFileReplicator;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.impl.FileContentInfoFilenameFactory;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class VfsClassLoaderTest {
  
  private static final String HDFS_URI = "hdfs://localhost:8020";
  private static final Path TEST_DIR = new Path(HDFS_URI + "/test-dir");

  private FileSystem hdfs = null;
  private DefaultFileSystemManager vfs = null;
  private VFSClassLoader cl = null;
  
  private static MiniDFSCluster cluster = null;
  private static Configuration conf = null;
  @BeforeClass
  public static void start() throws Exception {
    Logger.getRootLogger().setLevel(Level.ERROR);

    //Setup HDFS
    conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, HDFS_URI);
    conf.set("hadoop.security.token.service.use_ip", "true");
    conf.set("dfs.datanode.data.dir.perm", "775");
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024 * 100); //100K blocksize
    
    cluster = new MiniDFSCluster(8020, conf, 1, true, true, true, null, null, null, null);
    cluster.waitActive();
    
  }
  
  @AfterClass
  public static void stop() throws Exception {
    if (null != cluster)
      cluster.shutdown();
  }
  
  @Before
  public void setup() throws Exception {
    
    this.hdfs = cluster.getFileSystem();
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
  
  @Test
  public void testFileMonitor() throws Exception {
    MyFileMonitor listener = new MyFileMonitor();
    DefaultFileMonitor monitor = new DefaultFileMonitor(listener);
    monitor.setRecursive(true);
    FileObject testDir = vfs.resolveFile(TEST_DIR.toUri().toString());
    monitor.addFile(testDir);
    monitor.start();
    
    //Copy jar file to a new file name
    URL jarPath = this.getClass().getResource("/HelloWorld.jar");
    Path src = new Path(jarPath.toURI().toString());
    Path dst = new Path(TEST_DIR, "HelloWorld2.jar");
    this.hdfs.copyFromLocalFile(src, dst);

    Thread.sleep(4000);
    Assert.assertTrue(listener.isFileCreated());

    //Update the jar
    jarPath = this.getClass().getResource("/HelloWorld.jar");
    src = new Path(jarPath.toURI().toString());
    dst = new Path(TEST_DIR, "HelloWorld2.jar");
    this.hdfs.copyFromLocalFile(src, dst);

    Thread.sleep(4000);
    Assert.assertTrue(listener.isFileChanged());
    
    this.hdfs.delete(dst, false);
    Thread.sleep(4000);
    Assert.assertTrue(listener.isFileDeleted());
    
    monitor.stop();
    
  }
  
  
  @After
  public void tearDown() throws Exception {
    this.hdfs.delete(TEST_DIR, true);
    this.hdfs.close();
    this.vfs.close();
  }
  
  
  public static class MyFileMonitor implements FileListener {

    private boolean fileChanged = false;
    private boolean fileDeleted = false;
    private boolean fileCreated = false;
    
    public void fileCreated(FileChangeEvent event) throws Exception {
      System.out.println(event.getFile() + " created");
      this.fileCreated = true;
    }

    public void fileDeleted(FileChangeEvent event) throws Exception {
      System.out.println(event.getFile() + " deleted");
      this.fileDeleted = true;
    }

    public void fileChanged(FileChangeEvent event) throws Exception {
      System.out.println(event.getFile() + " changed");
      this.fileChanged = true;
    }

    public boolean isFileChanged() {
      return fileChanged;
    }

    public boolean isFileDeleted() {
      return fileDeleted;
    }

    public boolean isFileCreated() {
      return fileCreated;
    }
    
    
  }
}
