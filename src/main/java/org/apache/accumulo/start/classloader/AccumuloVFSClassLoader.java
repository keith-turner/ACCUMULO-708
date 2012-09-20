package org.apache.accumulo.start.classloader;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.vfs2.CacheStrategy;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.cache.DefaultFilesCache;
import org.apache.commons.vfs2.cache.SoftRefFilesCache;
import org.apache.commons.vfs2.impl.DefaultFileReplicator;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.impl.FileContentInfoFilenameFactory;
import org.apache.commons.vfs2.provider.ReadOnlyHdfsFileProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * This class builds a hierarchy of Classloaders in the form of:
 * 
 * <pre>
 * SystemClassLoader that loads JVM classes
 *       ^
 *       |
 * URLClassLoader that references the URLs for HADOOP_HOME, ZOOKEEPER_HOME, ACCUMULO_HOME and their associated directories
 *       ^
 *       |
 * AccumuloContextClassLoader that contains a map of context names to AccumuloReloadingVFSClassLoaders
 * </pre>
 * 
 * This class requires new properties in the site configuration file
 * 
 * default.context.classpath -> list of URIs for the default system context.
 * classloader.context.names -> name1, name2, name3
 * <name>.context.classpath -> list of URIs for this context
 * 
 */
public class AccumuloVFSClassLoader {

  private static final Logger log = Logger.getLogger(AccumuloVFSClassLoader.class);
  
  public static final String DEFAULT_CLASSPATH_PROPERTY = "default.context.classpath";
  
  public static final String CONTEXT_NAMES_PROPERTY = "classloader.context.names";
  
  public static final String CONTEXT_CLASSPATH_PROPERTY = ".context.classpath";
  
  //TODO: Need to get this from the config
  public static final String DEFAULT_CLASSPATH = "hdfs://localhost:8020/accumulo/classpath";
  
  private static DefaultFileSystemManager vfs = null;
  private static ClassLoader parent = null;
  private static volatile AccumuloContextClassLoader loader = null;
  private static final Object lock = new Object();
  private static final Configuration ACC_CONF = new Configuration();;  
  private static final String SITE_CONF;
  static {
    String configFile = System.getProperty("org.apache.accumulo.config.file", "accumulo-site.xml");
    if (System.getenv("ACCUMULO_HOME") != null) {
      // accumulo home should be set
      SITE_CONF = System.getenv("ACCUMULO_HOME") + "/conf/" + configFile;
    } else {
      /*
       * if not it must be the build-server in which case I use a hack to get unittests working
       */
      String userDir = System.getProperty("user.dir");
      if (userDir == null)
        throw new RuntimeException("Property user.dir is not set");
      int index = userDir.indexOf("accumulo/");
      if (index >= 0) {
        String acuhome = userDir.substring(0, index + "accumulo/".length());
        SITE_CONF = acuhome + "/conf/" + configFile;
      } else {
        SITE_CONF = "/conf/" + configFile;
      }
    }
    try {
      ACC_CONF.addResource(new File(SITE_CONF).toURI().toURL());
    } catch (MalformedURLException e) {
      throw new RuntimeException("Unable to create configuration from accumulo-site file: " + SITE_CONF, e);
    }
  }

  /**
   * Replace environment variables in the classpath string with their actual value
   * 
   * @param classpath
   * @param env
   * @return
   */
  private static String replaceEnvVars(String classpath, Map<String,String> env) {
    Pattern envPat = Pattern.compile("\\$[A-Za-z][a-zA-Z0-9_]*");
    Matcher envMatcher = envPat.matcher(classpath);
    while (envMatcher.find(0)) {
      // name comes after the '$'
      String varName = envMatcher.group().substring(1);
      String varValue = env.get(varName);
      if (varValue == null) {
        varValue = "";
      }
      classpath = (classpath.substring(0, envMatcher.start()) + varValue + classpath.substring(envMatcher.end()));
      envMatcher.reset(classpath);
    }
    return classpath;
  }

  /**
   * Populate the list of URLs with the items in the classpath string
   * 
   * @param classpath
   * @param urls
   * @throws MalformedURLException
   */
  private static void addUrl(String classpath, ArrayList<URL> urls) throws MalformedURLException {
    classpath = classpath.trim();
    if (classpath.length() == 0)
      return;
    
    classpath = replaceEnvVars(classpath, System.getenv());
    
    // Try to make a URI out of the classpath
    URI uri = null;
    try {
      uri = new URI(classpath);
    } catch (URISyntaxException e) {
      // Not a valid URI
    }
    
    if (null == uri || !uri.isAbsolute() || (null != uri.getScheme() && uri.getScheme().equals("file://"))) {
      // Then treat this URI as a File.
      // This checks to see if the url string is a dir if it expand and get all jars in that directory
      final File extDir = new File(classpath);
      if (extDir.isDirectory())
        urls.add(extDir.toURI().toURL());
      else {
        if (extDir.getParentFile() != null) {
          File[] extJars = extDir.getParentFile().listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
              return name.matches("^" + extDir.getName());
            }
          });
          if (extJars != null && extJars.length > 0) {
            for (File jar : extJars)
              urls.add(jar.toURI().toURL());
          }
        }
      }
    } else {
      urls.add(uri.toURL());
    }
    
  }
    
  private static ArrayList<URL> findAccumuloURLs() throws IOException {
    String cp = ACC_CONF.get(AccumuloClassLoader.CLASSPATH_PROPERTY_NAME, AccumuloClassLoader.ACCUMULO_CLASSPATH_VALUE);
    if (cp == null)
      return new ArrayList<URL>();
    String[] cps = replaceEnvVars(cp, System.getenv()).split(",");
    ArrayList<URL> urls = new ArrayList<URL>();
    for (String classpath : cps) {
      if (!classpath.startsWith("#")) {
        addUrl(classpath, urls);
      }
    }
    return urls;
  }
  
  private static ClassLoader getAccumuloClassLoader() throws IOException {
    ClassLoader parentClassLoader = ClassLoader.getSystemClassLoader();
    ArrayList<URL> accumuloURLs = findAccumuloURLs();
    log.debug("Create 2nd tier ClassLoader using URLs: " + accumuloURLs.toString());
    URLClassLoader aClassLoader = new URLClassLoader(accumuloURLs.toArray(new URL[accumuloURLs.size()]), parentClassLoader);
    return aClassLoader;
  }

  public synchronized static <U> Class<? extends U> loadClass(String classname, Class<U> extension) throws ClassNotFoundException {
    try {
      return (Class<? extends U>) Class.forName(classname, true, getClassLoader()).asSubclass(extension);
    } catch (IOException e) {
      throw new ClassNotFoundException("IO Error loading class " + classname, e);
    }
  }
  
  public static Class<?> loadClass(String classname) throws ClassNotFoundException {
    return loadClass(classname, Object.class).asSubclass(Object.class);
  }

  public static ClassLoader getClassLoader() throws IOException {
    ClassLoader localLoader = loader;
    while (null == localLoader) {
      synchronized (lock) {
        if (null == loader) {
          
          if (null == vfs) {
            vfs = new DefaultFileSystemManager();
            //TODO: Might be able to use a different cache impl or specify cache directory in configuration.
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
                    
          //Set up the 2nd tier class loader
          if (null == parent)
            parent = getAccumuloClassLoader();
          
          //Get the context names and classpaths from the configuration
          String[] defaultPaths = ACC_CONF.getStrings(DEFAULT_CLASSPATH_PROPERTY, DEFAULT_CLASSPATH);
          FileObject[] defaultClassPath = new FileObject[defaultPaths.length];
          int i = 0;
          for (String path : defaultPaths) {
            FileObject fo = vfs.resolveFile(path);
            defaultClassPath[i++] = fo;
          }
          
          //Create the Accumulo Context ClassLoader using the DEFAULT_CONTEXT
          loader = new AccumuloContextClassLoader(defaultClassPath, vfs, parent);

          //Add the other contexts
          String[] contexts = ACC_CONF.getStrings(CONTEXT_NAMES_PROPERTY);
          if (null != contexts) {
            for (String context : contexts) {
              String[] contextPaths = ACC_CONF.getStrings(context + CONTEXT_CLASSPATH_PROPERTY);
              if (null != contextPaths) {
                FileObject[] contextClassPath = new FileObject[contextPaths.length];
                for (String cp : contextPaths) {
                  FileObject fo = vfs.resolveFile(cp);
                  contextClassPath[i++] = fo;
                }
                log.debug("Creating Context ClassLoader for context: " + context + " using paths: " + contextClassPath.toString());
                loader.addContext(context, contextClassPath);
              }
            }
          }
        }
      }
      localLoader = loader;
    }
    return localLoader;
  }
  
  //TODO: Need to put this in a shutdown hook
  public static void close() {
    if (null != vfs)
      vfs.close();
  }
  
}
