package com.sparrow.stream.utils.redis;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

public class AddressAssemble {
    /**
     * @param address
     * @return
     */
    public static Set<InetSocketAddress> assemble(String address) {
        String[] ipPorts = address.split(",");
        Set<InetSocketAddress> inetSocketAddresses = new HashSet<>(ipPorts.length);
        for (String ipPort : ipPorts) {
            String[] ipPortPair = ipPort.split(":");
            inetSocketAddresses.add(new InetSocketAddress(ipPortPair[0], Integer.valueOf(ipPortPair[1])));
        }
        return inetSocketAddresses;
    }
}
