package org.apache.commons.vfs2.provider;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsFileObject extends AbstractFileObject {

  private FileSystem hdfs = null;
  private Path path = null;
  
  protected HdfsFileObject(AbstractFileName name, ReadOnlyHdfsFileSystem fs, FileSystem hdfs, Path p) {
    super(name,fs);
    this.hdfs = hdfs;
    this.path = p;
  }
  
  @Override
  protected FileType doGetType() throws Exception {
    FileStatus stat = this.hdfs.getFileStatus(this.path);
    if (stat.isDir())
      return FileType.FOLDER;
    else
      return FileType.FILE;
  }

  @Override
  protected String[] doListChildren() throws Exception {
    FileStatus stat = this.hdfs.getFileStatus(this.path);
    if (stat.isDir()) {
      
      FileStatus[] files = this.hdfs.listStatus(this.path);
      String[] children = new String[files.length];
      int i = 0;
      for (FileStatus status : files) {
        children[i++] = status.getPath().getName();
      }
      return children;
    }
    return null;
  }

  @Override
  protected long doGetContentSize() throws Exception {
    FileStatus stat = this.hdfs.getFileStatus(this.path);
    if (!stat.isDir())
      return stat.getLen();
    return 0;
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    return this.hdfs.open(this.path);
  }

  @Override
  public void close() throws FileSystemException {
    super.close();
    try {
      this.hdfs.close();
    } catch (IOException e) {
      throw new FileSystemException("Error closing FileSystem", e);
    }
  }
  
  
}
