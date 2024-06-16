/*
版权所有 IBM Corp. 保留所有权利。

许可证标识符：Apache-2.0
*/

package kafka

import "github.com/Shopify/sarama"

// staticPartitioner 结构体用于实现静态分区器，始终选择指定的分区ID。
type staticPartitioner struct {
	partitionID int32 // 静态分配的分区编号
}

// newStaticPartitioner 返回一个分区器构造函数，该构造函数创建的分区器会始终选择给定的分区。
func newStaticPartitioner(partition int32) sarama.PartitionerConstructor {
	return func(topic string) sarama.Partitioner {
		// 返回一个静态分区器实例，其中包含预先设定的分区ID
		return &staticPartitioner{partition}
	}
}

// Partition 方法接收一个生产者消息和分区总数，然后选择一个分区。
// 对于静态分区器，总是返回预先设定的分区ID。
func (prt *staticPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	// 直接返回静态设定的分区ID，忽略numPartitions参数
	return prt.partitionID, nil
}

// RequiresConsistency 方法表明分区器是否需要键到分区的映射保持一致性。
// 对于静态分区器，由于总是选择固定的分区，因此此方法返回true，表明其操作具有一致性。
func (prt *staticPartitioner) RequiresConsistency() bool {
	return true
}
