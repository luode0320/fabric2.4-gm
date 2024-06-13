/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"strconv"
	"strings"
	"time"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/spf13/viper"
)

const (
	defaultExecutionTimeout = 30 * time.Second
	minimumStartupTimeout   = 5 * time.Second
)

type Config struct {
	TotalQueryLimit int             // 限制每个查询返回的记录数
	TLSEnabled      bool            // 是否开启tls
	Keepalive       time.Duration   // 在通信通过不支持keep-alive的代理，此参数将保持连接在peer和chaincode之间。
	ExecuteTimeout  time.Duration   // Invoke和Init调用的超时持续时间，以防止失控。
	InstallTimeout  time.Duration   // 等待chaincode构建和安装过程完成的最长持续时间。
	StartupTimeout  time.Duration   // 启动容器并等待Register注册通过的超时持续时间。
	LogFormat       string          // 日志格式化
	LogLevel        string          // 日志级别
	ShimLogLevel    string          // 覆盖 'shim' 记录器的日志级别
	SCCAllowlist    map[string]bool // 启用系统链码
}

func GlobalConfig() *Config {
	c := &Config{}
	c.load()
	return c
}

func (c *Config) load() {
	viper.SetEnvPrefix("CORE")
	viper.AutomaticEnv()
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)
	// 是否开启tls
	c.TLSEnabled = viper.GetBool("peer.tls.enabled")
	// 在通信通过不支持keep-alive的代理，此参数将保持连接在peer和chaincode之间。
	c.Keepalive = toSeconds(viper.GetString("chaincode.keepalive"), 0)
	// Invoke和Init调用的超时持续时间，以防止失控。
	c.ExecuteTimeout = viper.GetDuration("chaincode.executetimeout")
	if c.ExecuteTimeout < time.Second {
		c.ExecuteTimeout = defaultExecutionTimeout
	}

	// 等待chaincode构建和安装过程完成的最长持续时间。
	c.InstallTimeout = viper.GetDuration("chaincode.installTimeout")
	// 启动容器并等待Register注册通过的超时持续时间。
	c.StartupTimeout = viper.GetDuration("chaincode.startuptimeout")
	if c.StartupTimeout < minimumStartupTimeout {
		c.StartupTimeout = minimumStartupTimeout
	}

	// 启用系统链码
	c.SCCAllowlist = map[string]bool{}
	for k, v := range viper.GetStringMapString("chaincode.system") {
		c.SCCAllowlist[k] = parseBool(v)
	}

	// 日志格式化、级别
	c.LogFormat = viper.GetString("chaincode.logging.format")
	c.LogLevel = getLogLevelFromViper("chaincode.logging.level")
	c.ShimLogLevel = getLogLevelFromViper("chaincode.logging.shim")

	// 限制每个查询返回的记录数
	c.TotalQueryLimit = 10000 // need a default just in case it's not set
	if viper.IsSet("ledger.state.totalQueryLimit") {
		c.TotalQueryLimit = viper.GetInt("ledger.state.totalQueryLimit")
	}
}

func parseBool(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "true", "t", "1", "enable", "enabled", "yes":
		return true
	default:
		return false
	}
}

func toSeconds(s string, def int) time.Duration {
	seconds, err := strconv.Atoi(s)
	if err != nil {
		return time.Duration(def) * time.Second
	}

	return time.Duration(seconds) * time.Second
}

// getLogLevelFromViper gets the chaincode container log levels from viper
func getLogLevelFromViper(key string) string {
	levelString := viper.GetString(key)
	if !flogging.IsValidLevel(levelString) {
		chaincodeLogger.Warningf("%s has invalid log level %s. defaulting to %s", key, levelString, flogging.DefaultLevel())
		levelString = flogging.DefaultLevel()
	}

	return flogging.NameToLevel(levelString).String()
}

// DevModeUserRunsChaincode enables chaincode execution in a development
// environment
const DevModeUserRunsChaincode string = "dev"

// IsDevMode returns true if the peer was configured with development-mode
// enabled.
func IsDevMode() bool {
	mode := viper.GetString("chaincode.mode")

	return mode == DevModeUserRunsChaincode
}
