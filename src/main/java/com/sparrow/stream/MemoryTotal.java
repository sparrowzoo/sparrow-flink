package com.sparrow.stream;

public class MemoryTotal {
    public static void main(String[] args) {
        String memory = "[{\"skuId\":8903,\"time\":1581161749669},{\"skuId\":120132,\"time\":1581161733253},{\"skuId\":219753,\"time\":1581161704368},{\"skuId\":2933232,\"time\":1581161577061},{\"skuId\":2386368,\"time\":1581161364573},{\"skuId\":2889567,\"time\":1580992906558},{\"skuId\":2402529,\"time\":1580992336032}]";
        int bytes = 800 * 2000000;
        System.out.println(bytes / 1024 / 1024 / 1024D);
    }
}
