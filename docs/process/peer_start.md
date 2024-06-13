## Peer 节点启动

### 1. peer启动入口
- [cmd/peer/main.go](../../cmd/peer/main.go)

### 2. 启动peer

- 注意配置 hosts (`C:\Windows\System32\drivers\etc\hosts`)
```shell
127.0.0.1 peer0.org.dns.com
127.0.0.1 peer1.org.dns.com
127.0.0.1 orderer0.dns.com
127.0.0.1 orderer2.dns.com
127.0.0.1 orderer1.dns.com
127.0.0.1 chaincode.server.com 
```

- 使用`.run`目录下的idea启动文件启动

`.run/peer0 7150 7250 7350.run.xml`

`.run/peer1 7151 7251 7351.run.xml`

![启动配置.png](img/启动配置.png)


### 3. 配置文件环境变量 FABRIC_CFG_PATH

[sampleconfig=linux配置目录](../../sampleconfig)

[samplewindowsconfig=windows配置目录](../../samplewindowsconfig)


初始化函数默认指定了 linux 与 windows 的环境变量为配置目录下的文件夹

## 注意事项

[sampleconfig](../../sampleconfig) 与 [samplewindowsconfig](../../samplewindowsconfig)
文件夹中的var属于账本数据, 第一次启动异常, 如果有var目录可以删除var目录