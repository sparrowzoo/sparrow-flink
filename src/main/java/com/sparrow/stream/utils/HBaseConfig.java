package com.sparrow.stream.utils;

public class HBaseConfig {
    // rpc调用超时时间
    private Long rpcTimeout;
    // 写缓冲区大小
    private Integer batchWriteBuffer;
    // 客户端重试次数
    private Integer clientRetriesNumber;
    // 客户端pause时间
    private Long clientPause;
    // zkQuorum zookeeper地址，多个要用逗号分隔
    private String quorum;
    // parent
    private String parent;

    public Long getRpcTimeout() {
        return rpcTimeout;
    }

    public void setRpcTimeout(Long rpcTimeout) {
        this.rpcTimeout = rpcTimeout;
    }

    public Integer getBatchWriteBuffer() {
        return batchWriteBuffer;
    }

    public void setBatchWriteBuffer(Integer batchWriteBuffer) {
        this.batchWriteBuffer = batchWriteBuffer;
    }

    public Integer getClientRetriesNumber() {
        return clientRetriesNumber;
    }

    public void setClientRetriesNumber(Integer clientRetriesNumber) {
        this.clientRetriesNumber = clientRetriesNumber;
    }

    public Long getClientPause() {
        return clientPause;
    }

    public void setClientPause(Long clientPause) {
        this.clientPause = clientPause;
    }
    public String getQuorum() {
        return quorum;
    }

    public void setQuorum(String quorum) {
        this.quorum = quorum;
    }

    public String getParent() {
        return parent;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }
}
