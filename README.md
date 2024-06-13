# Hyperledger Fabric [![join the chat][rocketchat-image]][rocketchat-url]
version: V2.3.x

[rocketchat-url]: https://chat.hyperledger.org/channel/fabric
[rocketchat-image]: https://open.rocket.chat/images/join-chat.svg

[![Build Status](https://dev.azure.com/Hyperledger/Fabric/_apis/build/status/Merge?branchName=main)](https://dev.azure.com/Hyperledger/Fabric/_build/latest?definitionId=51&branchName=main)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/955/badge)](https://bestpractices.coreinfrastructure.org/projects/955)
[![Go Report Card](https://goreportcard.com/badge/github.com/hyperledger/fabric)](https://goreportcard.com/report/github.com/hyperledger/fabric)
[![GoDoc](https://godoc.org/github.com/hyperledger/fabric?status.svg)](https://godoc.org/github.com/hyperledger/fabric)
[![Documentation Status](https://readthedocs.org/projects/hyperledger-fabric/badge/?version=latest)](http://hyperledger-fabric.readthedocs.io/en/latest)

这个项目是一个_毕业_Hyperledger项目。有关此项目历史的更多信息，请参见 [Fabric wiki页面](https://wiki.hyperledger.org/display/fabric)。关于什么_毕业_需要的信息可以在
[Hyperledger项目生命周期文档](https://tsc.hyperledger.org/project-lifecycle.html)。
Hyperledger Fabric是分布式账本解决方案的平台，支持
通过提供高度保密性的模块化架构，
弹性、灵活性和可扩展性。它旨在支持可插拔
不同组件的实现，并适应复杂性和
经济生态系统中存在的复杂性。

Hyperledger Fabric提供了独特的弹性和可扩展的体系结构，
将其与替代区块链解决方案区分开来。规划
企业区块链的未来需要建立在经过全面审查的基础上，
开源架构; Hyperledger Fabric是您的起点。

## Releases

Fabric大约每四个月提供一次具有新功能的发布
和改进。此外，某些版本被指定为长期版本
支持 (LTS) 版本。重要的修复程序将被反向移植到最新的
LTS释放，以及在LTS释放重叠期间之前的LTS释放。
有关更多详细信息，请参见 [LTS策略](https://github.com/hyperledger/fabric-rfcs/blob/main/text/0005-lts-release-strategy.md)。

LTS发布:
-[v2.2.x](https://hyperledger-fabric.readthedocs.io/en/release-2.2/whatsnew.html) (当前LTS版本)
-[v1.4.x](https://hyperledger-fabric.readthedocs.io/en/release-1.4/whatsnew.html) (先前的LTS版本，通过2021年4月维护)

除非另有说明，否则所有版本都可以从先前的次要版本升级。
此外，每个LTS版本都可以升级到下一个LTS版本。

可以在 [GitHub版本页面](https://github.com/hyperledger/fabric/releases) 上找到结构版本和版本说明。

请访问 [带有Epic标签的GitHub问题](https://github.com/hyperledger/fabric/labels/Epic) 以获取我们的发布路线图。
关注RocketChat [# fabric-release](https://chat.hyperledger.org/channel/fabric-release) 频道的发布讨论。

## 文档、入门和开发人员指南

请访问我们的
在线文档
有关使用和开发fabric，SDK和chaincode的信息:
- [v2.4](http://hyperledger-fabric.readthedocs.io/en/release-2.4/)
- [v2.3](http://hyperledger-fabric.readthedocs.io/en/release-2.3/)
- [v2.2](http://hyperledger-fabric.readthedocs.io/en/release-2.2/)
- [v2.1](http://hyperledger-fabric.readthedocs.io/en/release-2.1/)
- [v2.0](http://hyperledger-fabric.readthedocs.io/en/release-2.0/)
- [v1.4](http://hyperledger-fabric.readthedocs.io/en/release-1.4/)
- [v1.3](http://hyperledger-fabric.readthedocs.io/en/release-1.3/)
- [v1.2](http://hyperledger-fabric.readthedocs.io/en/release-1.2/)
- [v1.1](http://hyperledger-fabric.readthedocs.io/en/release-1.1/)
- [main branch (development)](http://hyperledger-fabric.readthedocs.io/en/latest/)

建议初次使用的用户首先浏览文档的 “入门” 部分，以熟悉Hyperledger Fabric组件和基本事务流程。

# # 贡献

我们欢迎以多种形式为Hyperledger Fabric项目做出贡献。
总是有很多事情要做!查看 [有关如何为该项目做出贡献的文档](http:// hyperledger-fabric.readthedocs.io/en/latest/CONTRIBUTING.html)
详细信息。

## 社区

[Hyperledger Community](https://www.hyperledger.org/community)

[Hyperledger mailing lists and archives](http://lists.hyperledger.org/)

[Hyperledger Chat](http://chat.hyperledger.org/channel/fabric)

[Hyperledger Fabric Issue Tracking (GitHub Issues)](https://github.com/hyperledger/fabric/issues)

[Hyperledger Fabric Wiki](https://wiki.hyperledger.org/display/Fabric)

[Hyperledger Wiki](https://wiki.hyperledger.org/)

[Hyperledger Code of Conduct](https://wiki.hyperledger.org/display/HYP/Hyperledger+Code+of+Conduct)

[Community Calendar](https://wiki.hyperledger.org/display/HYP/Calendar+of+Public+Meetings)

## 许可证 <a name="license"></a>

Hyperledger项目源代码文件可在Apache许可证版本2.0 (Apache-2.0) 下使用，该许可证位于 [许可证] (许可证) 文件中。Hyperledger项目文档文件可在知识共享署名4.0国际许可证 (CC-BY-4.0) 下获得，网址为http://creativecommons.org/licenses/by/4.0/。