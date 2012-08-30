package org.apache.commons.vfs2.provider;

import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;

public class HdfsFileSystemConfigBuilder extends FileSystemConfigBuilder {

  private final static HdfsFileSystemConfigBuilder BUILDER = new HdfsFileSystemConfigBuilder(); 
  
  private String hdfsUri = null;
  
  public String getHdfsUri() {
    return hdfsUri;
  }

  public void setHdfsUri(String hdfsUri) {
    this.hdfsUri = hdfsUri;
  }

  @Override
  protected Class<? extends FileSystem> getConfigClass() {
    return ReadOnlyHdfsFileSystem.class;
  }
  
  public static HdfsFileSystemConfigBuilder getInstance() {
    return BUILDER;
  }
  
}
