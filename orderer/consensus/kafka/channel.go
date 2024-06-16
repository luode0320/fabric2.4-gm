/*
版权所有 IBM Corp. 2016 All Rights Reserved.

根据Apache许可证2.0版（"许可证"）授权；您不能使用此文件，除非符合许可证条款。
您可以通过以下网址获得许可证的副本：

         http://www.apache.org/licenses/LICENSE-2.0

除非适用法律要求或书面同意，否则根据许可证分发的软件是按"原样"分发的，
不附带任何明示或暗示的保证或条件。有关管理权限和
许可证限制，请参阅许可证中的特定语言。
*/

// kafka包提供了与Kafka消息代理交互的功能，特别是用于基于Kafka的排序服务。

package kafka

import "fmt"

const defaultPartition = 0 // 默认的Kafka分区编号

// channel接口定义了一个通道，标识了Kafka基于的排序服务与其交互的Kafka分区。
type channel interface {
	topic() string    // 返回Kafka主题名称
	partition() int32 // 返回Kafka分区号
	fmt.Stringer      // 实现String方法，用于字符串表示
}

// channelImpl结构体实现了channel接口，包含了Kafka主题和分区的信息。
type channelImpl struct {
	tpc string // Kafka主题名称
	prt int32  // Kafka分区编号
}

// newChannel函数根据给定的主题名和分区号创建一个新的channel实例。
func newChannel(topic string, partition int32) channel {
	return &channelImpl{
		tpc: topic,
		prt: partition,
	}
}

// topic方法返回该通道所属的Kafka主题名称。
func (chn *channelImpl) topic() string {
	return chn.tpc
}

// partition方法返回该通道所在的Kafka分区编号。
func (chn *channelImpl) partition() int32 {
	return chn.prt
}

// String方法返回一个字符串，标识与此通道对应的Kafka主题/分区。
func (chn *channelImpl) String() string {
	return fmt.Sprintf("%s/%d", chn.tpc, chn.prt)
}
