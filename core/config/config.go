/*
Copyright Greg Haskins <gregory.haskins@gmail.com> 2017, All Rights Reserved.
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"fmt"
	"github.com/spf13/viper"
	"os"
	"path/filepath"
	"runtime"
)

func dirExists(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fi.IsDir()
}

// AddConfigPath 函数用于添加配置路径。
// 参数：
//   - v *viper.Viper：要添加配置路径的 Viper 实例，如果为 nil，则添加到全局的 Viper 实例。
//   - p string：要添加的配置路径。
func AddConfigPath(v *viper.Viper, p string) {
	if v != nil {
		v.AddConfigPath(p)
	} else {
		viper.AddConfigPath(p)
	}
}

// AddLocalViperConfigPaths 函数用于添加配置路径。
// 参数：
//   - v *viper.Viper：要添加配置路径的 Viper 实例，如果为 nil，则添加到全局的 Viper 实例。
//   - p string：要添加的配置路径。
func AddLocalViperConfigPaths(v *viper.Viper) {
	v.AddConfigPath(os.Getenv("FABRIC_CFG_PATH"))
	v.AddConfigPath(Current)
	if dirExists(CurrentDefault) {
		v.AddConfigPath(CurrentDefault)
	}
	if dirExists(OfficialPath) {
		v.AddConfigPath(OfficialPath)
	}
	// 根据操作系统确定 peerConfig 的路径
	switch runtime.GOOS {
	case "linux":
		// 2. 自定义的 ./sampleconfig
		if dirExists(PeerLinuxConfig) {
			v.AddConfigPath(PeerLinuxConfig)
		}
		if dirExists(PeerLinuxDefault) {
			v.AddConfigPath(PeerLinuxDefault)
		}
	case "windows":
		// 3. 自定义的 ./samplewindowsconfig
		if dirExists(PeerWindowsConfig) {
			v.AddConfigPath(PeerWindowsConfig)
		}
		if dirExists(PeerWindowsDefault) {
			v.AddConfigPath(PeerWindowsDefault)
		}
	}
}

// AddViperConfigPaths 函数用于添加配置路径。
// 参数：
//   - v *viper.Viper：要添加配置路径的 Viper 实例，如果为 nil，则添加到全局的 Viper 实例。
//   - p string：要添加的配置路径。
func AddViperConfigPaths() {
	viper.AddConfigPath(os.Getenv("FABRIC_CFG_PATH"))
	viper.AddConfigPath(Current)
	if dirExists(CurrentDefault) {
		viper.AddConfigPath(CurrentDefault)
	}
	if dirExists(OfficialPath) {
		viper.AddConfigPath(OfficialPath)
	}
	// 根据操作系统确定 peerConfig 的路径
	switch runtime.GOOS {
	case "linux":
		// 2. 自定义的 ./sampleconfig
		if dirExists(PeerLinuxConfig) {
			viper.AddConfigPath(PeerLinuxConfig)
		}
		if dirExists(PeerLinuxDefault) {
			viper.AddConfigPath(PeerLinuxDefault)
		}
	case "windows":
		// 3. 自定义的 ./samplewindowsconfig
		if dirExists(PeerWindowsConfig) {
			viper.AddConfigPath(PeerWindowsConfig)
		}
		if dirExists(PeerWindowsDefault) {
			viper.AddConfigPath(PeerWindowsDefault)
		}
	}
}

// TranslatePath 函数用于将相对路径转换为相对于指定配置文件的完全限定路径。绝对路径将原样返回。
// 参数：
//   - base string：指定配置文件的路径。
//   - p string：相对路径。
//
// 返回值：
//   - string：转换后的完全限定路径。
func TranslatePath(base, p string) string {
	// 如果路径是绝对路径，则直接返回
	if filepath.IsAbs(p) {
		return p
	}
	// 否则，将相对路径与指定配置文件的路径拼接起来，返回完全限定路径
	return filepath.Join(base, p)
}

// ----------------------------------------------------------------------------------
// TranslatePathInPlace()
// ----------------------------------------------------------------------------------
// Translates a relative path into a fully qualified path in-place (updating the
// pointer) relative to the config file that specified it.  Absolute paths are
// passed unscathed.
// ----------------------------------------------------------------------------------
func TranslatePathInPlace(base string, p *string) {
	*p = TranslatePath(base, *p)
}

// GetPath 函数用于获取配置字符串中指定的相对路径。
// 参数：
//   - key string：配置字符串的键。
//
// 返回值：
//   - string：解析后的绝对路径。
func GetPath(key string) string {
	p := viper.GetString(key)
	if p == "" {
		return ""
	}

	return TranslatePath(filepath.Dir(viper.ConfigFileUsed()), p)
}

// GetLocalPath 函数用于获取配置字符串中指定的相对路径。
// 参数：
//   - key string：配置字符串的键。
//
// 返回值：
//   - string：解析后的绝对路径。
func GetLocalPath(v *viper.Viper, key string) string {
	p := v.GetString(key)
	if p == "" {
		return ""
	}

	return TranslatePath(filepath.Dir(v.ConfigFileUsed()), p)
}

// OfficialPath 官方 FABRIC_CFG_PATH环境变量
const OfficialPath = "/etc/hyperledger/fabric"
const Current = "./"
const CurrentDefault = "./default"

// PeerLinuxConfig 自定义的 FABRIC_CFG_PATH环境变量
const PeerLinuxConfig = "./sampleconfig"
const PeerLinuxDefault = "./sampleconfig/default"

// PeerWindowsConfig 自定义的 FABRIC_CFG_PATH环境变量
const PeerWindowsConfig = "./samplewindowsconfig"
const PeerWindowsDefault = "./samplewindowsconfig/default"

// InitViper 函数用于执行基本的 viper 配置层初始化。
// 主要目的是建立应该被查询以找到所需配置的路径。
// 如果 v == nil，则会初始化全局的 Viper 实例。
// 参数：
//   - v *viper.Viper：要初始化的 Viper 实例，如果为 nil，则初始化全局的 Viper 实例。
//   - configName string：默认的配置文件的名称(如果没有找到同名的配置)。
//
// 返回值：
//   - error：如果初始化过程中出现错误，则返回错误。
func InitViper(v *viper.Viper, configName string) error {
	// 如果没有使用环境变量覆盖路径，我们将按照优先顺序使用默认路径：
	if v != nil {
		AddLocalViperConfigPaths(v)
	} else {
		AddViperConfigPaths()
	}

	// 现在设置配置文件的名称。
	if v != nil {
		// 如果用户使用与启动文件相同的名称, 并以yaml为文件后缀的配置, 我们优先考虑这个配置
		appNameWithoutExt := GetConfig()
		fmt.Printf("使用配置文件: %s \n", appNameWithoutExt)
		v.SetConfigName(appNameWithoutExt)
		if err := v.ReadInConfig(); err == nil {
			v.SetConfigName(appNameWithoutExt)
		} else {
			fmt.Printf("使用默认配置文件: %s", configName)
			v.SetConfigName(configName)
		}
	} else {
		appNameWithoutExt := GetConfig()
		// 如果用户使用与启动文件相同的名称, 并以yaml为文件后缀的配置, 我们优先考虑这个配置
		fmt.Printf("使用配置文件: %s \n", appNameWithoutExt)
		viper.SetConfigName(appNameWithoutExt)
		if err := viper.ReadInConfig(); err == nil {
			viper.SetConfigName(appNameWithoutExt)
		} else {
			fmt.Printf("使用默认配置文件: %s", configName)
			viper.SetConfigName(configName)
		}
	}

	return nil
}

// InitViperDefault 函数用于执行基本的 viper 配置层初始化。
// 主要目的是建立应该被查询以找到所需配置的路径。
// 如果 v == nil，则会初始化全局的 Viper 实例。
// 参数：
//   - v *viper.Viper：要初始化的 Viper 实例，如果为 nil，则初始化全局的 Viper 实例。
//   - configName string：指定的配置文件的名称。
//
// 返回值：
//   - error：如果初始化过程中出现错误，则返回错误。
func InitViperDefault(v *viper.Viper, configName string) error {
	if v != nil {
		AddLocalViperConfigPaths(v)
	} else {
		AddViperConfigPaths()
	}

	// 现在设置配置文件的名称。
	if v != nil {
		v.SetConfigName(configName)
	} else {
		viper.SetConfigName(configName)
	}

	return nil
}
