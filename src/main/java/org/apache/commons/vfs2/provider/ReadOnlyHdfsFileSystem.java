package org.apache.commons.vfs2.provider;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadOnlyHdfsFileSystem extends AbstractFileSystem {

  private FileSystem fs = null;
  
  protected ReadOnlyHdfsFileSystem(FileName rootName, FileSystemOptions fileSystemOptions) {
    super(rootName, null, fileSystemOptions);
  }

  @Override
  public void close() {
    try {
      if (null != fs)
        fs.close();
    } catch (IOException e) {
      throw new RuntimeException("Error closing HDFS client", e);
    }
    super.close();
  }

  @Override
  protected FileObject createFile(AbstractFileName name) throws Exception {
    throw new FileSystemException("Operation not supported");
  }

  @Override
  protected void addCapabilities(Collection<Capability> capabilities) {
    capabilities.addAll(ReadOnlyHdfsFileProvider.capabilities);
  }

  @Override
  @SuppressWarnings("resource")
  public FileObject resolveFile(FileName name) throws FileSystemException {
    String hdfsUri = name.getRootURI();
    Configuration conf = new Configuration();
    conf.set(org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY, hdfsUri);
    org.apache.hadoop.fs.FileSystem fs;
    try {
      fs = org.apache.hadoop.fs.FileSystem.get(conf);
      Path filePath = new Path(name.getPath());
      return new HdfsFileObject((AbstractFileName) name, this, fs, filePath);
    } catch (IOException e) {
      throw new RuntimeException("Error connecting to filesystem", e);
    }
    
    
  }

  
}
