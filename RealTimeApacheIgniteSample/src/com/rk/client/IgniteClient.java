package com.rk.client;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import com.rk.domain.Employee;


public class IgniteClient {

	private static IgniteCache<Integer, Employee> ignitecache=null;
	private static List<UUID> uuid=null;
	public static void main(String[] args) throws InterruptedException {
		Ignite ignite=startIgnite();
		IgniteCluster cluster=ignite.cluster();
		UUID locNode=cluster.localNode().id();
		System.out.println("LocalNode id is......."+" "+locNode);
		//cluster.stopNodes(uuid);
		int count=0;
		IgniteCompute compute = ignite.compute();
		compute.broadcast(() -> System.out.println("Hello Server"+compute.clusterGroup().node().id()));
		for(int j=0; j<300; j++)
		{
			System.out.println(getCacheData(ignite,j));
			count++;
		}
		System.out.println("Toatal number of entry get from server cache..."+" "+count);
		//UUID nodeId=ignite.cluster().forClients().node().id();
		/*System.out.println("Trying to get client node id...."+" "+ignite.cluster().forClients().node().id());*/
		/*System.out.println("values are in server cache Before Time expired"+" "+e);
		System.out.println("===============================================================================");*/
		
		//after expired time
		/*Thread.sleep(8000);
		Employee e1=getCacheData(ignite,1);
		System.out.println("trying to fetch data after expiry time from server cache"+" "+e1);
		System.out.println("===============================================================================");*/
		//stopIgnite();
		//ignite.destroyCache("EmployeeCache");
		printServerNodes(cluster, ignite);
		/*Employee e3=getCacheData(ignite,1);
		System.out.println("trying to fetch data after expiry time from server cache"+" "+e3);*/
		removeNode(ignite);
		//stopIgnite();
	}
	
	public static Ignite startIgnite()
	{
		final IgniteConfiguration ignConfiguration=new IgniteConfiguration();
		ignConfiguration.setClientMode(true);
		ignConfiguration.setPeerClassLoadingEnabled(true);
		
		final TcpCommunicationSpi spi=new TcpCommunicationSpi();
		spi.setSocketWriteTimeout(60000);
		final TcpDiscoverySpi tcSpi=new TcpDiscoverySpi();
		final TcpDiscoveryVmIpFinder finder=new TcpDiscoveryVmIpFinder();
		finder.setAddresses(Arrays.asList("127.0.0.1:47500..47509","127.0.0.1:47500..47509"));
		tcSpi.setIpFinder(finder);
		ignConfiguration.setDiscoverySpi(tcSpi);
		ignConfiguration.setCommunicationSpi(spi);
		Ignite ignite=Ignition.start(ignConfiguration);
		return ignite;
	}
	
	
	 public static void stopIgnite()
	 {
         Ignition.stop(true);
	 } 
	 
	 public static Employee getCacheData(Ignite ignite,Integer id) throws InterruptedException
	 {
		 ignitecache=ignite.cache("EmployeeCache");
		
		/*//before removing from cache
			System.out.println("trying to fetch data before expiry time from cache"+ignitecache.get(1));
			System.out.println("trying to fetch data before expiry time from cache"+ignitecache.get(2));
			System.out.println("trying to fetch data before expiry time from cache"+ignitecache.get(3));*/
			
			System.out.println("===============================================================================");
			 /*//after expired time
			 Thread.sleep(5000);
			 System.out.println("trying to fetch data after expiry time from cache"+ignitecache.get(1));
			 System.out.println("trying to fetch data after expiry time from cache"+ignitecache.get(2));
			 System.out.println("trying to fetch data after expiry time from cache"+ignitecache.get(3));*/
			 
			 return ignitecache.get(id);
         
	 }
	 
	private static boolean switchingServerOnDelayTime()
	{
		
		return false;
	}
		
	private static void printServerNodes(IgniteCluster cluster, Ignite ignite) 
	{
		ClusterGroup remoteCluster = ignite.cluster().forRemotes();
		ignite.compute(remoteCluster);
		Collection<ClusterNode> remoteNodes = remoteCluster.nodes();
		for(ClusterNode node: remoteNodes) {
			System.out.println(node.toString()+ "is server: "+!node.isClient());
				ignite.cluster().setBaselineTopology(remoteNodes);
		}
		
		
	}
	
	private static Map<String,BaselineNode> getServerNode(Ignite ignite)
	{
		 Map<String, BaselineNode> nodes = new HashMap<>();
		/* ClusterGroup remotegrp=ignite.cluster().forRemotes();
		 ignite.compute(remotegrp);
		 Collection<ClusterNode> remoteNodes=remotegrp.nodes();*/
		 for(ClusterNode node: ignite.cluster().forServers().nodes())
		 {
			 nodes.put(node.consistentId().toString(),node);
			 System.out.println("Printing consistent id node...."+" "+node.hostNames().toString());
		 }
		 return nodes;
	}
	
	private static Map<String, BaselineNode> currentBaseLine(Ignite ignite) {
        Map<String, BaselineNode> nodes = new HashMap<>();
        
        Collection<BaselineNode> baseline = ignite.cluster().currentBaselineTopology();

        if (!F.isEmpty(baseline)) {
            for (BaselineNode node : baseline)
                nodes.put(node.consistentId().toString(), node);
        }

        return nodes;
	}
	
	private static boolean removeNode(Ignite ignite)
	{
		Map<String,BaselineNode> nodeInfo=getServerNode(ignite);
		if(nodeInfo.containsValue(ignite.cluster().forServers().nodes()))
		return true;
		return false;
	}
	
	private static boolean UpdateCacheData(Ignite ignite, Integer val)
	{
		IgniteCache<Integer, Employee> cacheign=ignite.cache("EmployeeCache");
		
		return false;
	}
	
}


