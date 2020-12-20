package com.lb.springboot.curator.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryOneTime;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @author lb
 * @date: 2020-12-20 17:36<br/>
 * @since JDK 1.8
 */
@Configuration
@Slf4j
public class ZkCuratorConfig {

    @Value("${zookeeper.cluster.hosts}")
    private String hosts;
    @Value("${zookeeper.cluster.sessionTimeout}")
    private Integer sessionTimeout;

    /**
     * 初始化zkClient
     */
    public CuratorFramework initZkClient(){
        CuratorFramework client = CuratorFrameworkFactory.builder()
                    .connectString(hosts)
                    .sessionTimeoutMs(sessionTimeout)
                    .retryPolicy(retryPolicy())
                    .build();
        log.info("=======initZkClient state: {} ========",client.getState().name());
        return client;
    }

    public CuratorFramework createRetryForeverClient(){
        return CuratorFrameworkFactory.builder()
                .connectString(hosts)
                .retryPolicy(new RetryForever(5000))
                .build();
    }

    /**
     * 重试策略
     * @return
     */
    private RetryPolicy retryPolicy(){
        // 重试3次，间隔1秒，至多睡眠时间10秒
        RetryPolicy backoffRetry = new BoundedExponentialBackoffRetry(1000,10000,3);
        // 重试5次，间隔1秒
        RetryPolicy exponentialBackoffRetry = new ExponentialBackoffRetry(1000,5);
        // 重试5次，间隔2秒
        RetryPolicy retryFiveTimes = new RetryNTimes(5,20000);
        // 重试1次，间隔3秒
        RetryPolicy retryOneTime = new RetryOneTime(3000);
        // 一直重试，间隔5秒
        RetryPolicy retryForever = new RetryForever(5000);
        return retryFiveTimes;
    }
}
