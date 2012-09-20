package org.apache.accumulo.start.classloader;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.SecureClassLoader;
import java.util.Enumeration;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.log4j.Logger;

/**
 * Classloader that delegates operations to a VFSClassLoader object. This class also listens
 * for changes in any of the files/directories that are in the classpath and will recreate
 * the delegate object if there is any change in the classpath.
 *
 */
public class AccumuloReloadingVFSClassLoader extends SecureClassLoader implements FileListener {
  
  private static final Logger log = Logger.getLogger(AccumuloReloadingVFSClassLoader.class);

  private FileObject[] files = null;
  private FileSystemManager vfs = null;
  private ClassLoader parent = null;
  private DefaultFileMonitor monitor = null;
  private volatile VFSClassLoader cl = null;
  private final ReentrantLock lock = new ReentrantLock(true);
  
  public AccumuloReloadingVFSClassLoader(FileObject[] files, FileSystemManager vfs, ClassLoader parent) throws FileSystemException {
    this.files = files;
    this.vfs = vfs;
    this.parent = parent;
    
    if (null != parent)
      cl = new VFSClassLoader(files, vfs, parent);
    else
      cl = new VFSClassLoader(files, vfs);
    
    monitor = new DefaultFileMonitor(this);
    monitor.setDelay(2000);
    monitor.setRecursive(true);
    for (FileObject file : files)
      monitor.addFile(file);
    monitor.start();
  }
  
  public FileObject[] getFiles() {
    return this.files;
  }
  
  /**
   * Should be ok if this is not called because the thread started by DefaultFileMonitor is a daemon thread
   */
  public void close() {
    monitor.stop();
  }

  public void fileCreated(FileChangeEvent event) throws Exception {
    lock.lock();
    try {
      if (log.isDebugEnabled())
        log.debug(event.getFile().getURL().toString() + " created, recreating classloader");
      cl = new VFSClassLoader(files, vfs, parent);
    } finally {
      lock.unlock();
    }
  }

  public void fileDeleted(FileChangeEvent event) throws Exception {
    lock.lock();
    try {
      if (log.isDebugEnabled())
        log.debug(event.getFile().getURL().toString() + " changed, recreating classloader");
      cl = new VFSClassLoader(files, vfs, parent);
    } finally {
      lock.unlock();
    }
  }

  public void fileChanged(FileChangeEvent event) throws Exception {
    lock.lock();
    try {
      if (log.isDebugEnabled())
        log.debug(event.getFile().getURL().toString() + " deleted, recreating classloader");
      cl = new VFSClassLoader(files, vfs, parent);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    lock.lock();
    try {
      return this.cl.loadClass(name);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public URL getResource(String name) {
    lock.lock();
    try {
      return this.cl.getResource(name);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    lock.lock();
    try {
      return this.cl.getResources(name);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    lock.lock();
    try {
      return this.cl.getResourceAsStream(name);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public synchronized void setDefaultAssertionStatus(boolean enabled) {
    lock.lock();
    try {
      this.cl.setDefaultAssertionStatus(enabled);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public synchronized void setPackageAssertionStatus(String packageName, boolean enabled) {
    lock.lock();
    try {
      this.cl.setPackageAssertionStatus(packageName, enabled);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public synchronized void setClassAssertionStatus(String className, boolean enabled) {
    lock.lock();
    try {
      this.cl.setClassAssertionStatus(className, enabled);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public synchronized void clearAssertionStatus() {
    lock.lock();
    try {
      this.cl.clearAssertionStatus();
    } finally {
      lock.unlock();
    }
  }
  
}
