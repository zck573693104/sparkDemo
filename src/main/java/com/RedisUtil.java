package com;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.SortingParams;

public class RedisUtil implements Serializable {

    private static final long serialVersionUID = -1503230506006497851L;
    private Jedis jedis;//非切片额客户端连接
    private JedisPool jedisPool;//非切片连接池
    private ShardedJedis shardedJedis;//切片额客户端连接
    private ShardedJedisPool shardedJedisPool;//切片连接池

    public RedisUtil() {
        initialPool();
        initialShardedPool();
        shardedJedis = shardedJedisPool.getResource();
        jedis = jedisPool.getResource();
    }

    /**
     * 初始化非切片池
     */
    private void initialPool() {
        // 池基本配置 
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(20);
        config.setMaxIdle(5);
        config.setMaxWaitMillis(1000L);
        config.setTestOnBorrow(false);

        jedisPool = new JedisPool(config, "127.0.0.1", 6379);
    }

    /**
     * 初始化切片池
     */
    private void initialShardedPool() {
        // 池基本配置 
        JedisPoolConfig config = new JedisPoolConfig();
        //config.setMaxActive(20); 旧版本
        config.setMaxTotal(20);
        config.setMaxIdle(5);
        //config.setMaxWait(1000l); 旧版本
        config.setMaxWaitMillis(1000L);
        config.setTestOnBorrow(false);
        // slave链接 
        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
        shards.add(new JedisShardInfo("127.0.0.1", 6379, "master"));

        // 构造池 
        shardedJedisPool = new ShardedJedisPool(config, shards);
    }
    public void setHash(String topic,int partition,Long offset){
        jedis.hset(topic,String.valueOf(partition),offset.toString());

    }

     public Long getHash(String topic,String partition){
        String value = jedis.hget(topic,partition);
        if (value!=null){
            return Long.valueOf(jedis.hget(topic,partition));
        }
        return 0L;

    }
}
