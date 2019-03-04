package com.sql;

import lombok.Data;

import java.io.Serializable;

@Data
public class ShopRating implements Serializable {
    private static final long serialVersionUID = 8161282786524316505L;
    private Integer skuId;
    private Integer rating;

    public ShopRating(Integer skuId, Integer rating) {
        this.skuId = skuId;
        this.rating = rating;
    }

    public ShopRating() {
    }
}
