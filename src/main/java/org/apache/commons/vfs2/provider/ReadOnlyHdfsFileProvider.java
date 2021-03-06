package org.apache.commons.vfs2.provider;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.http.HttpFileNameParser;

public class ReadOnlyHdfsFileProvider extends AbstractOriginatingFileProvider {

  public static final Collection<Capability> capabilities = Collections.unmodifiableCollection(Arrays.asList(new Capability[]
  {
      Capability.GET_TYPE,
      Capability.READ_CONTENT,
      Capability.URI,
      Capability.GET_LAST_MODIFIED,
      Capability.ATTRIBUTES,
      Capability.RANDOM_ACCESS_READ,
      Capability.DIRECTORY_READ_CONTENT,
      Capability.LIST_CHILDREN,
      Capability.RANDOM_ACCESS_READ,
  }));

  public ReadOnlyHdfsFileProvider() {
    super();
    this.setFileNameParser(HttpFileNameParser.getInstance());
  }
  
  public Collection<Capability> getCapabilities() {
    return capabilities;
  }

  @Override
  protected FileSystem doCreateFileSystem(FileName rootName, FileSystemOptions fileSystemOptions) throws FileSystemException {
    return new ReadOnlyHdfsFileSystem(rootName, fileSystemOptions);
  }

  @Override
  public FileSystemConfigBuilder getConfigBuilder() {
    return HdfsFileSystemConfigBuilder.getInstance();
  }
  
}
