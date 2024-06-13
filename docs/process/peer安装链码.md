## Peer 节点安装链码

### 1. peer安装链码
- [cmd/peer/main.go](../../cmd/peer/main.go)

### 2. 启动peer安装链码必须带有参数
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

`.run/peer0 安装链码.run.xml`

`.run/peer1 安装链码.run.xml`

![启动配置.png](img/启动配置.png)

### 3. 配置文件环境变量 FABRIC_CFG_PATH

[sampleconfig=linux配置目录](../../sampleconfig)

[samplewindowsconfig=windows配置目录](../../samplewindowsconfig)
