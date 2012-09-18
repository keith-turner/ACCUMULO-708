package org.apache.accumulo.start.classloader;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.commons.vfs2.provider.ReadOnlyHdfsFileProvider;
import org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;
import org.apache.log4j.Logger;

/**
 * Class that maintains a VFSClassloader per context. In this case, a context could be anything.
 * For example, there could be a SYSTEM context, which Accumulo uses to perform system level tasks.
 * There could also be a context for each user, which would allow different users to use different
 * implementations of iterators at scan time. Finally, there could be a table context, in which different
 * implementations of classes could be used for compactions.
 * 
 * 
 *  
 */
public class AccumuloVFSClassLoader extends AccumuloClassLoader {
  
  private static Map<String,ClassLoader> classLoaderCache = new ConcurrentHashMap<String,ClassLoader>();
  
  private static final Logger log = Logger.getLogger(AccumuloVFSClassLoader.class);
  
  public AccumuloVFSClassLoader() throws Exception {
    //Set up the VFS
    DefaultFileSystemManager vfs = new DefaultFileSystemManager();
    //vfs.setLogger(log);
    vfs.setDefaultProvider(new DefaultLocalFileProvider());
    vfs.addProvider("hdfs", new ReadOnlyHdfsFileProvider());
  }
  /**
   * 
   * @param name class name
   * @param context context name, will be created if it does not exist
   * @return requested class object
   * @throws ClassNotFoundException
   */
  public Class<?> loadClass(String name, String context) throws ClassNotFoundException {
    if (!classLoaderCache.containsKey(context)) {
//      VFSClassLoader classLoader = new VFSClassLoader();
//      classLoaderCache.put(context, new VFSClassLoader());
    }
    return classLoaderCache.get(context).loadClass(name);
  }
  
}
