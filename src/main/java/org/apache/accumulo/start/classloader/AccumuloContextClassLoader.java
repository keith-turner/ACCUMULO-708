package org.apache.accumulo.start.classloader;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.SecureClassLoader;
import java.util.Enumeration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;

/**
 * Classloader that delegates requests to a classloader mapped to a specific context
 * 
 *
 */
public class AccumuloContextClassLoader extends SecureClassLoader {
  
  public static final String DEFAULT_CONTEXT = "SYSTEM";

  private Map<String, AccumuloReloadingVFSClassLoader> loaders = new ConcurrentHashMap<String, AccumuloReloadingVFSClassLoader>();
  private FileSystemManager vfs = null;
  private ClassLoader parent = null;
  
  public AccumuloContextClassLoader(FileObject[] defaultClassPath, FileSystemManager vfs, ClassLoader parent) throws FileSystemException {
    this.vfs = vfs;
    this.parent = parent;
    loaders.put(DEFAULT_CONTEXT, new AccumuloReloadingVFSClassLoader(defaultClassPath, vfs, parent));
  }
  
  public synchronized void addContext(String context, FileObject[] files) throws FileSystemException {
    if (!loaders.containsKey(context))
      loaders.put(context, new AccumuloReloadingVFSClassLoader(files, vfs, parent));
  }
  
  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    return loadClass(DEFAULT_CONTEXT, name);
  }
  
  public Class<?> loadClass(String context, String name) throws ClassNotFoundException {
    if (null == context)
      context = DEFAULT_CONTEXT;
    return loaders.get(context).loadClass(name);
  }

  @Override
  public URL getResource(String name) {
    return getResource(DEFAULT_CONTEXT, name);
  }

  public URL getResource(String context, String name) {
    if (null == context)
      context = DEFAULT_CONTEXT;
    return loaders.get(context).getResource(name);
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    return getResources(DEFAULT_CONTEXT, name);
  }

  public Enumeration<URL> getResources(String context, String name) throws IOException {
    if (null == context)
      context = DEFAULT_CONTEXT;
    return loaders.get(context).getResources(name);
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    return getResourceAsStream(DEFAULT_CONTEXT, name);
  }

  public InputStream getResourceAsStream(String context, String name) {
    if (null == context)
      context = DEFAULT_CONTEXT;
    return loaders.get(context).getResourceAsStream(name);
  }

  @Override
  public synchronized void setDefaultAssertionStatus(boolean enabled) {
    setDefaultAssertionStatus(DEFAULT_CONTEXT, enabled);
  }

  public synchronized void setDefaultAssertionStatus(String context, boolean enabled) {
    if (null == context)
      context = DEFAULT_CONTEXT;
    loaders.get(context).setDefaultAssertionStatus(enabled);
  }

  @Override
  public synchronized void setPackageAssertionStatus(String packageName, boolean enabled) {
    setPackageAssertionStatus(DEFAULT_CONTEXT, packageName, enabled);
  }

  public synchronized void setPackageAssertionStatus(String context, String packageName, boolean enabled) {
    if (null == context)
      context = DEFAULT_CONTEXT;
    loaders.get(context).setPackageAssertionStatus(packageName, enabled);
  }

  @Override
  public synchronized void setClassAssertionStatus(String className, boolean enabled) {
    setClassAssertionStatus(DEFAULT_CONTEXT, className, enabled);
  }

  public synchronized void setClassAssertionStatus(String context, String className, boolean enabled) {
    if (null == context)
      context = DEFAULT_CONTEXT;
    loaders.get(context).setClassAssertionStatus(className, enabled);
  }

  @Override
  public synchronized void clearAssertionStatus() {
    clearAssertionStatus(DEFAULT_CONTEXT);
  }

  public synchronized void clearAssertionStatus(String context) {
    if (null == context)
      context = DEFAULT_CONTEXT;
    loaders.get(context).clearAssertionStatus();
  }
  
  public AccumuloReloadingVFSClassLoader getClassLoader(String context) {
    return loaders.get(context);
  }
  
  public void removeContext(String context) {
    if (null != context && !(DEFAULT_CONTEXT.equals(context))) {
      AccumuloReloadingVFSClassLoader cl = loaders.remove(context);
      cl.close();
    }
  }
  
  public void close() {
    for (Entry<String, AccumuloReloadingVFSClassLoader> e : loaders.entrySet()) {
      e.getValue().close();
    }
    loaders.clear();
  }
  
}
