package com.test.RPC;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class RPCClient {

	public static void main(String[] args) throws Exception {
		IRPCInterface proxy = RPC.getProxy(IRPCInterface.class, 1, new InetSocketAddress("localhost",
				8888), new Configuration());
		String s  = proxy.test("test");
		System.out.println("RPCClient.main()======>"+s);
	}
}
