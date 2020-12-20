package com.lb.springboot.curator;

import com.lb.springboot.curator.config.ZkCuratorConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.nio.charset.Charset;

@Slf4j
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ZkCuratorApplicationTests {

    @Autowired
    private ZkCuratorConfig zkCuratorConfig;

    private CuratorFramework curator;

    @BeforeAll
    public void createConn() {
        curator = zkCuratorConfig.initZkClient();
        // 建立zk连接
        curator.start();
        log.info("连接已创建！state: {}",curator.getState().name());
    }

    @AfterAll
    public void closeConn(){
        // 一定记得关闭连接
        curator.close();
        log.info("连接已关闭！state: {}",curator.getState().name());
    }

    @Test
    public void createNodeTest(){
        String path = "/curatorTestNode/testnode3/xxxnode3";
        try {
            // 创建节点
            curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path,"aaa".getBytes());
        } catch (Exception e) {
            log.error("create Node failure",e);
        }
        log.info("创建完成 node: {}",path);
    }

    @Test
    public void setDataTest() throws Exception {
        String path = "/curatorTestNode/testnode3";
        Stat stat = new Stat();
        byte[] data = curator.getData().storingStatIn(stat).forPath(path);
        log.info("修改前的值 :{}",new String(data,Charset.defaultCharset()));
        curator.setData()
                // 基于当前版本更新，如果版本不匹配，则不更新
                .withVersion(1)
                .forPath(path,"update5".getBytes(Charset.defaultCharset()));
        data = curator.getData().forPath(path);
        log.info("更新完成 path: {},修改后的值: {}",path,new String(data,Charset.defaultCharset()));
    }

    @Test
    public void deleteTest()throws Exception{
        String path ="/curatorTestNode";
        curator.delete()
                // 删除失败一直重试，知道删除成功
                .guaranteed()
                // 执行递归删除
                .deletingChildrenIfNeeded()
                // 指定版本删除，如果设置为-1，不论什么版本都删除
                .withVersion(-1)
                // 指定数据节点目录
                .forPath(path);
        log.info("删除成功 path: {}",path);
    }
}
