package org.apache.accumulo.start.classloader;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;


public class AccumuloVFSClassLoader {
  
  private static Map<String,ClassLoader> classLoaderCache = new HashMap<String,ClassLoader>();
  
  
  
  public static ClassLoader getClassLoader() throws Exception {
    //Get current user
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String username = ugi.getUserName();

    if (classLoaderCache.containsKey(username))
      return classLoaderCache.get(username);
    
    
  }
  
}
