package com.sparrow.stream.window.behivior;

public class UserBehaviorBO {
    private Integer companyId;
    private Integer skuId;
    private Long time;
    private String spm;
    private Integer count;

    public Integer getCompanyId() {
        return companyId;
    }

    public void setCompanyId(Integer companyId) {
        this.companyId = companyId;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Integer getSkuId() {
        return skuId;
    }

    public void setSkuId(Integer skuId) {
        this.skuId = skuId;
    }

    public String getSpm() {
        return spm;
    }

    public void setSpm(String spm) {
        this.spm = spm;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "UserBehaviorBO{" +
                "companyId=" + companyId +
                ", skuId=" + skuId +
                ", time=" + time +
                ", spm='" + spm + '\'' +
                ", count=" + count +
                "this=" + this.hashCode() +
                '}';
    }
}
