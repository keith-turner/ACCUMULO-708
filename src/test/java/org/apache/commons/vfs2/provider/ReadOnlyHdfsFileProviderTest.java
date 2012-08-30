package org.apache.commons.vfs2.provider;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReadOnlyHdfsFileProviderTest {
  
  private DefaultFileSystemManager manager = null;
  
  @Before
  public void setup() throws Exception {
  
    manager = new DefaultFileSystemManager();
    manager.addProvider("hdfs", new ReadOnlyHdfsFileProvider());
    manager.init();
  }
  
  @Test
  public void testInit() throws Exception {
    @SuppressWarnings("unused")
    FileObject fo = manager.resolveFile("hdfs://localhost:8020/accumulo-base.jar");
    System.out.println("file: " + fo);
  }
  
  @After
  public void destroy() {
    manager.close();
  }
  
}
