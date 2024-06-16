/*
版权所有 IBM Corp. 保留所有权利。

许可证标识符：Apache-2.0
*/

package kafka

import (
	"crypto/tls"
	"crypto/x509"

	localconfig "github.com/hyperledger/fabric/orderer/common/localconfig"

	"github.com/Shopify/sarama"
)

// newBrokerConfig 根据提供的配置选项创建一个新的 Sarama 配置对象，用于与 Kafka 交互。
func newBrokerConfig(
	tlsConfig localconfig.TLS, // TLS 配置
	saslPlain localconfig.SASLPlain, // SASL 平文认证配置
	retryOptions localconfig.Retry, // 重试选项
	kafkaVersion sarama.KafkaVersion, // Kafka 版本
	chosenStaticPartition int32) *sarama.Config {
	// 为请求头等预留较大的缓冲区大小，单位为字节。
	paddingDelta := 1 * 1024 * 1024

	// 初始化一个新的 Sarama 配置
	brokerConfig := sarama.NewConfig()

	// 设置消费者重试间隔
	brokerConfig.Consumer.Retry.Backoff = retryOptions.Consumer.RetryBackoff

	// 允许消费过程中返回错误
	brokerConfig.Consumer.Return.Errors = true

	// 设置元数据重试配置
	brokerConfig.Metadata.Retry.Backoff = retryOptions.Metadata.RetryBackoff
	brokerConfig.Metadata.Retry.Max = retryOptions.Metadata.RetryMax

	// 设置网络超时选项
	brokerConfig.Net.DialTimeout = retryOptions.NetworkTimeouts.DialTimeout
	brokerConfig.Net.ReadTimeout = retryOptions.NetworkTimeouts.ReadTimeout
	brokerConfig.Net.WriteTimeout = retryOptions.NetworkTimeouts.WriteTimeout

	// 配置 TLS
	brokerConfig.Net.TLS.Enable = tlsConfig.Enabled
	if brokerConfig.Net.TLS.Enable {
		// 解析公钥和私钥
		keyPair, err := tls.X509KeyPair([]byte(tlsConfig.Certificate), []byte(tlsConfig.PrivateKey))
		if err != nil {
			logger.Panic("无法解码公钥/私钥对:", err)
		}
		// 构建根证书池
		rootCAs := x509.NewCertPool()
		for _, certificate := range tlsConfig.RootCAs {
			if !rootCAs.AppendCertsFromPEM([]byte(certificate)) {
				logger.Panic("无法解析根证书颁发机构证书 (Kafka.Tls.RootCAs)")
			}
		}
		// 配置 TLS 客户端设置
		brokerConfig.Net.TLS.Config = &tls.Config{
			Certificates: []tls.Certificate{keyPair},
			RootCAs:      rootCAs,
			MinVersion:   tls.VersionTLS12,
			MaxVersion:   0, // 使用最新的 TLS 版本
		}
	}

	// 配置 SASL 平文认证
	brokerConfig.Net.SASL.Enable = saslPlain.Enabled
	if brokerConfig.Net.SASL.Enable {
		brokerConfig.Net.SASL.User = saslPlain.User
		brokerConfig.Net.SASL.Password = saslPlain.Password
	}

	// 生产者最大消息大小设置，考虑到缓冲区
	brokerConfig.Producer.MaxMessageBytes = int(sarama.MaxRequestSize) - paddingDelta

	// 设置生产者重试配置
	brokerConfig.Producer.Retry.Backoff = retryOptions.Producer.RetryBackoff
	brokerConfig.Producer.Retry.Max = retryOptions.Producer.RetryMax

	// 分区器设置，当前静态分配分区, 参数为一个固定的int, 所有消息都发送到这个int的分区
	brokerConfig.Producer.Partitioner = newStaticPartitioner(chosenStaticPartition)

	// 设置确认级别，WaitForAll 表示领导者会等待所有 ISR 节点收到消息后才发送 ACK
	brokerConfig.Producer.RequiredAcks = sarama.WaitForAll

	// 根据 Sarama 库的要求设置，确保生产者能接收到成功确认
	brokerConfig.Producer.Return.Successes = true

	// 设置 Kafka 版本
	brokerConfig.Version = kafkaVersion

	return brokerConfig
}
