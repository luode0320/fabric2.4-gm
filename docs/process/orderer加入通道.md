## orderer 节点加入通道

### 1. orderer加入通道入口
- [cmd/osnadmin/main.go](../../cmd/osnadmin/main.go)

### 2. 启动orderer节点必须带有参数
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

`.run/osadmin orderer0 加入通道.run.xml`

`.run/osadmin orderer1 加入通道.run.xml`

`.run/osadmin orderer2 加入通道.run.xml`

![启动配置.png](img/启动配置.png)

### 3. 配置文件环境变量 FABRIC_CFG_PATH

[sampleconfig=linux配置目录](../../sampleconfig)

[samplewindowsconfig=windows配置目录](../../samplewindowsconfig)
