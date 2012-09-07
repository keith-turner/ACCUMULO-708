package org.apache.commons.vfs2.provider;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.commons.vfs2.util.RandomAccessMode;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsFileObject extends AbstractFileObject {

  public static final String LAST_ACCESS_TIME = "LAST_ACCESS_TIME";
  public static final String BLOCK_SIZE = "BLOCK_SIZE";
  public static final String GROUP = "GROUP";
  public static final String OWNER = "OWNER";
  public static final String PERMISSIONS = "PERMISSIONS";
  public static final String LENGTH = "LENGTH";
  public static final String MODIFICATION_TIME = "MODIFICATION_TIME";
  
  private ReadOnlyHdfsFileSystem fs = null;
  private FileSystem hdfs = null;
  private Path path = null;
  private FileStatus stat = null;
  
  protected HdfsFileObject(AbstractFileName name, ReadOnlyHdfsFileSystem fs, FileSystem hdfs, Path p) {
    super(name,fs);
    this.fs = fs;
    this.hdfs = hdfs;
    this.path = p;
  }

  @Override
  protected void doAttach() throws Exception {
      this.stat = this.hdfs.getFileStatus(this.path);
  }

  @Override
  protected FileType doGetType() throws Exception {
    doAttach();
    if (stat.isDir())
      return FileType.FOLDER;
    else
      return FileType.FILE;
  }
  
  @Override
  public boolean exists() throws FileSystemException {
    try {
      doGetType();
      return true;
    } catch (Exception e) {
      return false;
    } 
  }

  @Override
  public boolean canRenameTo(FileObject newfile) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected String[] doListChildren() throws Exception {
      FileStatus[] files = this.hdfs.listStatus(this.path);
      String[] children = new String[files.length];
      int i = 0;
      for (FileStatus status : files) {
        children[i++] = status.getPath().getName();
      }
      return children;
  }

  @Override
  protected long doGetContentSize() throws Exception {
      return stat.getLen();
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    return this.hdfs.open(this.path);
  }

  @Override
  protected boolean doIsHidden() throws Exception {
    return false;
  }

  @Override
  protected boolean doIsReadable() throws Exception {
    return true;
  }

  @Override
  protected boolean doIsWriteable() throws Exception {
    return false;
  }

  @Override
  protected FileObject[] doListChildrenResolved() throws Exception {
    String[] children = doListChildren();
    FileObject[] fo = new FileObject[children.length];
    for (int i = 0; i < children.length; i++) {
      Path p = new Path(this.path, children[i]);
      fo[i] = this.fs.resolveFile(p.toUri().toString());
    }
    return fo;
  }

  @Override
  protected long doGetLastModifiedTime() throws Exception {
    if (null != this.stat)
      return this.stat.getModificationTime();
    else
      return -1;
  }

  @Override
  protected boolean doSetLastModifiedTime(long modtime) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Map<String,Object> doGetAttributes() throws Exception {
    if (null == this.stat)
      return super.doGetAttributes();
    else {
      Map<String,Object> attrs = new HashMap<String,Object>();
      attrs.put(LAST_ACCESS_TIME, this.stat.getAccessTime());
      attrs.put(BLOCK_SIZE, this.stat.getBlockSize());
      attrs.put(GROUP, this.stat.getGroup());
      attrs.put(OWNER, this.stat.getOwner());
      attrs.put(PERMISSIONS, this.stat.getPermission().toString());
      attrs.put(LENGTH, this.stat.getLen());
      attrs.put(MODIFICATION_TIME, this.stat.getModificationTime());
      return attrs;
    }
  }

  @Override
  protected void doSetAttribute(String attrName, Object value) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void doRemoveAttribute(String attrName) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  protected RandomAccessContent doGetRandomAccessContent(RandomAccessMode mode) throws Exception {
    if (mode.equals(RandomAccessMode.READWRITE))
      throw new UnsupportedOperationException();
    return new HdfsReadOnlyRandomAccessContent(this.path, this.hdfs);
  }

  @Override
  protected boolean doIsSameFile(FileObject destFile) throws FileSystemException {
    throw new UnsupportedOperationException();
  }
  
}
