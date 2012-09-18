package org.apache.commons.vfs2.provider;

import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileContentInfo;
import org.apache.commons.vfs2.FileContentInfoFactory;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.impl.DefaultFileContentInfo;

public class HdfsReadOnlyFileContentInfoFactory implements FileContentInfoFactory {

  public FileContentInfo create(FileContent fileContent) throws FileSystemException {
    //TODO: Need to figure out a way to get this information from the file.
    String content = "text/plain";
    String encoding = "UTF-8";
    return new DefaultFileContentInfo(content, encoding);
  }
  
}
