package org.apache.accumulo.start.classloader;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.vfs2.CacheStrategy;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.cache.DefaultFilesCache;
import org.apache.commons.vfs2.cache.SoftRefFilesCache;
import org.apache.commons.vfs2.impl.DefaultFileReplicator;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.impl.FileContentInfoFilenameFactory;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.commons.vfs2.provider.ReadOnlyHdfsFileProvider;
import org.apache.log4j.Logger;

/**
 * Class that maintains a VFSClassloader per context. In this case, a context could be anything.
 * For example, there could be a SYSTEM context, which Accumulo uses to perform system level tasks.
 * There could also be a context for each user, which would allow different users to use different
 * implementations of iterators at scan time. Finally, there could be a table context, in which different
 * implementations of classes could be used for compactions.
 * 
 */
public class AccumuloVFSClassLoader extends AccumuloClassLoader {
  
  private static Map<String,ClassLoader> classLoaderCache = new ConcurrentHashMap<String,ClassLoader>();
  
  private static final Logger log = Logger.getLogger(AccumuloVFSClassLoader.class);

  private static final String DEFAULT_CONTEXT = "SYSTEM";

  private DefaultFileSystemManager vfs = null;
  
  public AccumuloVFSClassLoader() throws Exception {
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
  }
  /**
   * 
   * @param name class name
   * @param context context name, will be created if it does not exist
   * @return requested class object
   * @throws ClassNotFoundException
   */
  public Class<?> loadClass(String name, String context) throws ClassNotFoundException {
    if (null == context)
      context = DEFAULT_CONTEXT;
    
    if (!classLoaderCache.containsKey(context)) {
      //Determine the directories/jar files that should be in this context
      FileObject[] contextClassPath = getClassPathForContext(context);
      VFSClassLoader classLoader;
      try {
        classLoader = new VFSClassLoader(contextClassPath, this.vfs);
        classLoaderCache.put(context, classLoader);
      } catch (FileSystemException e) {
        throw new ClassNotFoundException("Error creating VFSClassLoader for class: " + name, e);
      }
    }
    return classLoaderCache.get(context).loadClass(name);
  }
  
  private FileObject[] getClassPathForContext(String context) {
    //TODO: Need to pull directories, jar file paths, etc. for context.
    return null;
  }
  
  @Override
  protected void finalize() throws Throwable {
    this.vfs.close();
    super.finalize();
  }
  
}
