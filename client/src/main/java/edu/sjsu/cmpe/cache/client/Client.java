package cache.client;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.base.Charsets;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class Client {
    
    private static final Funnel<CharSequence> strFunnel = Funnels.stringFunnel(Charset.defaultCharset());
    private final static  HashFunction hasher=Hashing.md5();
    public static ArrayList<String>listofservers=new ArrayList<String>();
    public static  SortedMap<Long,String>serversBucket=new TreeMap<Long,String>();
    public static  ArrayList objectsToAdd=new ArrayList ();
    
    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client- In Progress");
        
        
        //  consistentHashing();
        RezenvendousHashing();
        
        
        
        
        System.out.println("Exiting Cache Client ");
    }
    
    /**
     * Code for Consistent Hashing
     */
    public static void consistentHashing(){
        intializeVariables();
        
        for(int i=0;i<objectsToAdd.size();i++){
            
            String node=getServersForConsistentHash(Hashing.md5().hashString(objectsToAdd.get(i).toString(), Charsets.UTF_8).padToLong(),serversBucket);
            
            CacheServiceInterface cache = new DistributedCacheService(
                                                                      node);
            
        	   cache.put(i+1, objectsToAdd.get(i).toString());
            System.out.println("put("+(i+1)+" => " +objectsToAdd.get(i)+") to server "+node);
        }
        
        
        for(int i=0;i<objectsToAdd.size();i++){
            
            
            String node=getServersForConsistentHash(Hashing.md5().hashString(objectsToAdd.get(i).toString(), Charsets.UTF_8).padToLong(),serversBucket);
            
        	   CacheServiceInterface cache = new DistributedCacheService(
                                                                         node);
        	   String value=cache.get(i+1);
            System.out.println("get("+(i+1)+") => "+value+" from server "+node);
        }
    }
    
    
    /**
     * Code for Rezevendous Hashing
     */
    
    public static void RezenvendousHashing(){
        intializeVariables();
        
        for(int i=0;i<objectsToAdd.size();i++){
            
            
            
            
            String node=getServersForRhash(objectsToAdd.get(i).toString());
            
            
            CacheServiceInterface cache = new DistributedCacheService(
                                                                      node);
            
        	   cache.put(i+1, objectsToAdd.get(i).toString());
            System.out.println("put("+(i+1)+" => " +objectsToAdd.get(i)+") to server "+node);
        }
        
        
        for(int i=0;i<objectsToAdd.size();i++){
            
            
            
            String node=getServersForRhash(objectsToAdd.get(i).toString());
        	   CacheServiceInterface cache = new DistributedCacheService(
                                                                         node);
        	   String value=cache.get(i+1);
            System.out.println("get("+(i+1)+") => "+value+" from server "+node );
        }
    }
    
    /**
     * Initializa the variabales
     */
    
    public static void intializeVariables(){
        
        listofservers.add("http://localhost:3000");
        listofservers.add("http://localhost:3001");
        listofservers.add("http://localhost:3002");
        
        
        for(int i=0;i<listofservers.size();i++){
            
            serversBucket.put(Hashing.md5().hashString(listofservers.get(i), Charsets.UTF_8).padToLong(), listofservers.get(i));
        }
        
        objectsToAdd.add('a');
        objectsToAdd.add('b');
        objectsToAdd.add('c');
        objectsToAdd.add('d');
        objectsToAdd.add('e');
        objectsToAdd.add('f');
        objectsToAdd.add('g');
        objectsToAdd.add('h');
        objectsToAdd.add('i');
        objectsToAdd.add('j');
    }
    /**
     * Getting Node using Consistent hashing
     * @param hashfun
     * @param servermapping
     * @return
     */
    public static String getServersForConsistentHash(Long hashfun,SortedMap<Long,String>servermapping){
        
        
        if(!servermapping.containsKey(hashfun)){
            SortedMap<Long, String> tailMap =servermapping.tailMap(hashfun);
            
            hashfun=tailMap.isEmpty() ? servermapping.firstKey() : tailMap.firstKey();
            
        }
        
        return servermapping.get(hashfun);
    }
    
    
    /**
     * Getting the node value from Rendezvous or Highest Random Weight (HRW) hashing
     * @param key
     * @return
     */
    public static String getServersForRhash(String key) {
        long maxValue = Long.MIN_VALUE;
        String max = null;
        for (String node : listofservers) {
            long nodesHash = hasher.newHasher()
            .putObject(key, strFunnel)
            .putObject(node, strFunnel)
            .hash().asLong();
            if (nodesHash > maxValue) {
                max = node;
                maxValue = nodesHash;
            }
        }
        return max;
    }
    
    
}