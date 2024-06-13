package main

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
)

/**
 * 测试插件加载失败
 */
func TestPluginLoadingFailure(t *testing.T) {
	// 创建Gomega断言对象的代码
	gt := NewGomegaWithT(t)

	// 编译Peer节点的可执行文件, 编译Go程序并返回可执行文件的路径
	peer, err := gexec.Build("cmd/peer")
	fmt.Println("Listening on:", peer)
	gt.Expect(err).NotTo(HaveOccurred())
	// 清理构建工件
	defer gexec.CleanupBuildArtifacts()

	// 接受一个相对路径作为参数，并返回该路径的绝对路径
	parentDir, err := filepath.Abs("../..")
	fmt.Println("Listening on:", parentDir)
	gt.Expect(err).NotTo(HaveOccurred())

	// 指定的目录中创建一个临时目录，并返回该临时目录的路径, 指定目录为""则使用默认系统临时目录
	tempDir, err := ioutil.TempDir("", "plugin-failure")
	gt.Expect(err).NotTo(HaveOccurred())
	fmt.Println("Listening on:", tempDir)
	// 删除临时目录, RemoveAll移除路径及其包含的任何子路径
	defer os.RemoveAll(tempDir)

	// 创建三个TCP监听器用于模拟Peer节点的监听地址
	peerListener, err := net.Listen("tcp", "localhost:0")
	gt.Expect(err).NotTo(HaveOccurred())
	peerListenAddress := peerListener.Addr()
	fmt.Println("Listening on:", peerListenAddress.String())

	chaincodeListener, err := net.Listen("tcp", "localhost:0")
	gt.Expect(err).NotTo(HaveOccurred())
	chaincodeListenAddress := chaincodeListener.Addr()
	fmt.Println("Listening on:", chaincodeListenAddress.String())

	operationsListener, err := net.Listen("tcp", "localhost:0")
	gt.Expect(err).NotTo(HaveOccurred())
	operationsListenAddress := operationsListener.Addr()
	fmt.Println("Listening on:", operationsListenAddress.String())

	// 关闭这三个监听器
	err = peerListener.Close()
	gt.Expect(err).NotTo(HaveOccurred())
	err = chaincodeListener.Close()
	gt.Expect(err).NotTo(HaveOccurred())
	err = operationsListener.Close()
	gt.Expect(err).NotTo(HaveOccurred())

	// 使用循环遍历两个插件类型（`ENDORSERS_ESCC`和`VALIDATORS_VSCC`）
	strings := []string{"ENDORSERS_ESCC", "VALIDATORS_VSCC"}

	for _, plugin := range strings {
		plugin := plugin
		t.Run(plugin, func(t *testing.T) {
			// 使用`exec.Command`创建一个执行Peer节点的命令，并设置相应的环境变量; 创建了一个*exec.Cmd对象，用于执行一个命令
			cmd := exec.Command(peer, "node", "start")
			//`CORE_PEER_HANDLERS_XXX_LIBRARY`指定了插件文件的路径
			cmd.Env = []string{
				fmt.Sprintf("CORE_PEER_FILESYSTEMPATH=%s", tempDir),
				fmt.Sprintf("CORE_LEDGER_SNAPSHOTS_ROOTDIR=%s", filepath.Join(tempDir, "snapshots")),
				fmt.Sprintf("CORE_PEER_HANDLERS_%s_LIBRARY=%s", plugin, filepath.Join(parentDir, "internal/peer/testdata/invalid_plugins/invalidplugin.so")),
				fmt.Sprintf("CORE_PEER_LISTENADDRESS=%s", peerListenAddress),
				fmt.Sprintf("CORE_PEER_CHAINCODELISTENADDRESS=%s", chaincodeListenAddress),
				fmt.Sprintf("CORE_PEER_MSPCONFIGPATH=%s", "msp"),
				fmt.Sprintf("CORE_OPERATIONS_LISTENADDRESS=%s", operationsListenAddress),
				"CORE_OPERATIONS_TLS_ENABLED=false",
				fmt.Sprintf("FABRIC_CFG_PATH=%s", filepath.Join(parentDir, "sampleconfig")),
			}
			// 使用`gexec.Start`函数启动Peer节点，并使用Gomega断言对象验证Peer节点的行为
			sess, err := gexec.Start(cmd, nil, nil)
			gt.Expect(err).NotTo(HaveOccurred())
			// 使用Gomega断言对象验证Peer节点的输出是否包含预期的错误信息
			gt.Eventually(sess, time.Minute).Should(gexec.Exit(2))

			gt.Expect(sess.Err).To(gbytes.Say(fmt.Sprintf("panic: Error opening plugin at path %s", filepath.Join(parentDir, "internal/peer/testdata/invalid_plugins/invalidplugin.so"))))
			gt.Expect(sess.Err).To(gbytes.Say("plugin.Open"))
		})
	}
}
