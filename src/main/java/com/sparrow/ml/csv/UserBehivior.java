package com.sparrow.ml.csv;

import java.io.Serializable;

public class UserBehivior implements Serializable {
    public UserBehivior(Long userId, Long itemId, Double score) {
        this.userId = userId;
        this.itemId = itemId;
        this.score = score;
    }

    private Long userId;
    private Long itemId;
    private Double score;

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }
}
