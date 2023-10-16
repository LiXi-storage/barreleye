// Author: Li Xi
//
// Agent for HA of Clownfish.
//
package main

import (
	"math/rand"
	"strings"
	"time"
	"os"
	"os/signal"
	"os/exec"
	"syscall"
	"fmt"
	"sync"
	"bytes"
	"sort"
	yaml "gopkg.in/yaml.v2"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-uuid"
	"github.com/pelletier/go-toml"
)

const (
	// lockPath is the path used to acquire a coordinating lock
	// for a highly-available deploy.
	SessionTTLSeconds = 10
)

type LustreService struct {
	// If OST/MDT, then lustre-MDT0000 or lustre-OST0000
	// If MGS then mgs_id in clownfish.conf
	LSServiceName string
}

type LustreServiceInstance struct {
	LSIHostName string `toml:"hostname" yaml:"hostname"`
	LSIDevice string `toml:"device" yaml:"device"`
	LSINid string `toml:"nid" yaml:"nid"`
	LSIMnt string `toml:"mnt" yaml:"mnt"`
	LSService *LustreService
}

type LustreMdt struct {
	LMdtIndex int `toml:"index" yaml:"index"`
	LMdtInstances []LustreServiceInstance `toml:"instances" yaml:"instances"`
	LMdtService LustreService
}

type LustreOst struct {
	LOstIndex int `toml:"index" yaml:"index"`
	LOstInstances []LustreServiceInstance `toml:"instances" yaml:"instances"`
	LOstService LustreService
}

type LustreMgs struct {
	LMgsID string `toml:"mgs_id" yaml:"mgs_id"`
	LMgsInstances []LustreServiceInstance `toml:"instances" yaml:"instances"`
	LMgsService LustreService
}

type LustreFileSystem struct {
	LFFsname string `toml:"fsname" yaml:"fsname"`
	LFMdts []LustreMdt `toml:"mdts" yaml:"mdts"`
	LFOsts []LustreOst `toml:"osts" yaml:"osts"`
}

type SSHHost struct {
	SSHHostName string `toml:"hostname" yaml:"hostname"`
	SSHStandalone bool `toml:"standalone" yaml:"standalone"`
}

type ClownfishConfig struct {
	CCLustres []LustreFileSystem `toml:"filesystems" yaml:"filesystems"`
	CCMgsList []LustreMgs `toml:"mgs_list" yaml:"mgs_list"`
	CCSSHHosts []SSHHost `toml:"hosts" yaml:"hosts"`
}

type SSHHostList []SSHHost
func (list SSHHostList) Len() int {
	return len(list)
}

func (list SSHHostList) Less(i, j int) bool {
	return list[i].SSHHostName < list[j].SSHHostName
}

func (list SSHHostList) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

type RuntimeConfig struct {
	RCAutostartEnabled bool `yaml:"autostart"`
}

func lustreOSTIndex2String(indexNumber int) (string, error) {
	if indexNumber > 0xffff {
		return "", fmt.Errorf("too big index number: %d", indexNumber)
	}
	indexString := fmt.Sprintf("OST%04x", indexNumber)
	return indexString, nil
}

func lustreMDTIndex2String(indexNumber int) (string, error) {
	if indexNumber > 0xffff {
		return "", fmt.Errorf("too big index number: %d", indexNumber)
	}
	indexString := fmt.Sprintf("MDT%04x", indexNumber)
	return indexString, nil
}

func loadConfig(logger log.Logger) (*ClownfishConfig, error) {
	file := CLOWNF_CONFIG
	cmd := exec.Command("clownf", "simple_config")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()

	if err != nil {
		stdoutString := stdout.String()
		stderrString := stderr.String()
		stdoutString = strings.Replace(stdoutString, "\n", "\\n", -1)
		stderrString = strings.Replace(stderrString, "\n", "\\n", -1)
		logger.Error("failed to convert Clownfish config to simple version",
			     "error", err, "stdout", stdoutString,
			     "stderr", stderrString)
		return nil, err
	}

	conf := new(ClownfishConfig)
	tomlFile := stdout.Bytes()
	err = toml.Unmarshal(tomlFile, conf)
	if err != nil {
		logger.Error("failed to unmarshal config file as toml format", "file", file, "error", err)
		return nil, err
	}

	// Init the other field of config
	for _, lustrefs := range conf.CCLustres {
		for i := range lustrefs.LFOsts {
			ost := &lustrefs.LFOsts[i]
			ostIndexString, err := lustreOSTIndex2String(ost.LOstIndex)
			if err != nil {
				logger.Error("failed to get index string of OST", "error", err)
				return nil, err
			}
			serviceName := lustrefs.LFFsname + "-" + ostIndexString
			ost.LOstService.LSServiceName = serviceName
			for i := range ost.LOstInstances {
				ost.LOstInstances[i].LSService = &ost.LOstService
			}
		}

		for i := range lustrefs.LFMdts {
			mdt := &lustrefs.LFMdts[i]
			mdtIndexString, err := lustreMDTIndex2String(mdt.LMdtIndex)
			if err != nil {
				logger.Error("failed to get index string of MDT", "error", err)
				return nil, err
			}
			serviceName := lustrefs.LFFsname + "-" + mdtIndexString
			mdt.LMdtService.LSServiceName = serviceName
			for i := range mdt.LMdtInstances {
				mdt.LMdtInstances[i].LSService = &mdt.LMdtService
			}
		}
	}

	for i := range conf.CCMgsList {
		mgs := &conf.CCMgsList[i]
		mgs.LMgsService.LSServiceName = mgs.LMgsID
		for i := range mgs.LMgsInstances {
			mgs.LMgsInstances[i].LSService = &mgs.LMgsService
		}
	}

	return conf, nil
}

type LocalServiceInstances struct {
	LSServiceInstances []LustreServiceInstance
}

func filterLocalServices(logger log.Logger, config *ClownfishConfig) (*LocalServiceInstances, error) {
	localHostname, err := os.Hostname()
	if err != nil {
		logger.Error("failed to get local hostname", "error", err)
		return nil, err
	}
	logger.Debug("got local hostname", "hostname", localHostname)

	local := new(LocalServiceInstances)

	for _, lustrefs := range config.CCLustres {
		for _, ost := range lustrefs.LFOsts {
			for _, osti := range ost.LOstInstances {
				hostname := osti.LSIHostName
				if hostname == localHostname {
					logger.Debug("adding service", "device", osti.LSIDevice)
					local.LSServiceInstances = append(local.LSServiceInstances, osti)
				}
			}
		}

		for _, mdt := range lustrefs.LFMdts {
			for _, mdti := range mdt.LMdtInstances {
				hostname := mdti.LSIHostName
				if hostname == localHostname {
					logger.Debug("adding service", "device", mdti.LSIDevice)
					local.LSServiceInstances = append(local.LSServiceInstances, mdti)
				}
			}
		}
	}

	for _, mgs := range config.CCMgsList {
		for _, mgsi := range mgs.LMgsInstances {
			hostname := mgsi.LSIHostName
			if hostname == localHostname {
				logger.Debug("adding service", "device", mgsi.LSIDevice)
				local.LSServiceInstances = append(local.LSServiceInstances, mgsi)
			}
		}
	}
	return local, nil
}

type ServiceStatus string

const (
	SSUnknown ServiceStatus = "unknown"
	SSMounted ServiceStatus = "mounted"
	SSMountFailed ServiceStatus = "mount failed"
	//SSUmounting ServiceStatus = "umounting"
	//SSUmounted ServiceStatus = "umounted"
	//SSUmountFailed ServiceStatus = "umount failed"
)

type ServiceAgent struct {
	// Service name this agent is managing
	SAServiceName string
	// Client to connect to Consul
	SAConsulClient *api.Client
	// Status of the service
	SAServiceStatus ServiceStatus
	SASessionTTL time.Duration
	SASessionTTLSeconds int
	SAUuid string
	SAConsulLock *api.Lock
	SALockKey string
	SAConfigKey string
	SARuntimeConfig RuntimeConfig
}

func (agent *ServiceAgent) SAMaintainService(logger log.Logger,
					     waitGroup *sync.WaitGroup,
					     exitChannel <-chan struct{}) {
	waitGroup.Add(1)
	defer waitGroup.Done()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var newStatus ServiceStatus

	status := SSUnknown
	agent.SAServiceStatus = SSUnknown
	var autostartEnabled bool
	var oldAutostartEnabled bool
	first := true
	for {
		autostartEnabled = agent.SARuntimeConfig.RCAutostartEnabled
		if first || oldAutostartEnabled != autostartEnabled {
			if autostartEnabled {
				logger.Info("auotostart is enabled", "service", agent.SAServiceName)
			} else {
				logger.Info("auotostart is disabled", "service", agent.SAServiceName)
			}
			first = false
		}
		oldAutostartEnabled = autostartEnabled
		if autostartEnabled {
			if status == SSUnknown || status == SSMountFailed {
				logger.Info("starting service", "service", agent.SAServiceName)
			}
			cmd := exec.Command("clownf", "service", "mount", agent.SAServiceName)
			stdout.Reset()
			stderr.Reset()
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			startTime := time.Now()
			err := cmd.Run()
			duration := int(time.Since(startTime).Seconds())
			stdoutString := stdout.String()
			stdoutString = strings.Replace(stdoutString, "\n", "\\n", -1)
			stderrString := stderr.String()
			stderrString = strings.Replace(stderrString, "\n", "\\n", -1)

			if err != nil {
				logger.Error("failed to start service",
					     "service", agent.SAServiceName,
					     "error", err,
					     "stdout", stdoutString,
					     "stderr", stderrString,
					     "duration", duration)
				newStatus = SSMountFailed
			} else {
				newStatus = SSMounted
			}

			if status != newStatus {
				logger.Info("status change of service", "service", agent.SAServiceName,
					    "old", status, "new", newStatus,
					     "stdout", stdoutString,
					     "stderr", stderrString,
					     "duration", duration)
				status = newStatus
				agent.SAServiceStatus = newStatus
			} else if (newStatus == SSMounted &&
				   stdout.String() != MSG_ALREADY_MOUNTED) {
				logger.Info("mounted service with stale status of mounted",
					    "service", agent.SAServiceName,
					     "stdout", stdoutString,
					     "stderr", stderrString,
					     "duration", duration)
			}
		}

		select {
		case <-time.After(agent.SASessionTTL):
		case <-exitChannel:
			logger.Error("exiting from maintaining service", "service", agent.SAServiceName)
			return
		}
	}
}

// Read the lock value if lock is held. Return error if lock is not held.
func (agent *ServiceAgent) SAGetLockValue(path string) (bool, string, error) {
	client := agent.SAConsulClient

        kv := client.KV()
	queryOptions := &api.QueryOptions {
		RequireConsistent: true,
	}

        pair, _, err := kv.Get(path, queryOptions)
        if err != nil {
                return false, "", err
        }

        if pair == nil {
                return false, "", nil
        }

        held := pair.Session != ""
        value := string(pair.Value)
        return held, value, nil
}

func (agent *ServiceAgent) SAGetLeaderUUID() (string, error) {
	held, leaderUUID, err := agent.SAGetLockValue(agent.SALockKey)
	if err != nil {
		return "", err
	}

	if !held {
		return "", fmt.Errorf("failed to get readlock")
	}

	return leaderUUID, nil
}

func (agent *ServiceAgent) SAGetLeaderUUIDWait(exitChannel <-chan struct{}) (string, bool) {
	for i := 0; i < agent.SASessionTTLSeconds; i++ {
		leaderUUID, err := agent.SAGetLeaderUUID()
		if err == nil {
			return leaderUUID, false
		}

		select {
		case <-time.After(1 * time.Second):
			continue
		case <-exitChannel:
			return "", true
		}
	}
	return "", false
}

// SAAcquireLock blocks until the lock is acquired, returning the leaderLostCh
func (agent *ServiceAgent) SAAcquireLock(exitChannel <-chan struct{}) (<-chan struct{}) {
	for {
		// Attempt lock acquisition
		leaderLostCh, err := agent.SAConsulLock.Lock(exitChannel)
		if err == nil {
			return leaderLostCh
		}

		// Retry the acquisition
		select {
		case <-time.After(time.Duration(agent.SASessionTTLSeconds) * time.Second):
		case <-exitChannel:
			return nil
		}
	}
}

func (agent *ServiceAgent) SAMonitorServiceOnce(logger log.Logger,
						waitGroup *sync.WaitGroup,
						exitChannel <-chan struct{}) (bool) {
	var err error
	currentLeader, exiting := agent.SAGetLeaderUUIDWait(exitChannel)
	if exiting {
		logger.Info("exiting when trying to get the leader uuid",
			    "service", agent.SAServiceName)
		return true
	}

	// If the leader is not me, cleanup.
	// Note that the currentLeader could be empty string
	if currentLeader != agent.SAUuid {
		if currentLeader == "" {
			logger.Info("not able to get the current leader for a long time",
				    "service", agent.SAServiceName)
		} else {
			logger.Info("the current leader is someone else",
				    "service", agent.SAServiceName)
		}
	}

	logger.Info("trying to get the leadership lock",
		    "service", agent.SAServiceName)
	leaderLostCh := agent.SAAcquireLock(exitChannel)
	if leaderLostCh == nil {
		logger.Info("exiting when trying to get the leadership lock",
			    "service", agent.SAServiceName)
		return true
	}

	logger.Info("got the leadership lock", "service", agent.SAServiceName)
	go agent.SAMaintainService(logger, waitGroup, exitChannel)

	// Monitor a loss of leadership
	select {
	case <-leaderLostCh:
		logger.Info("lost the leadership",
			    "service", agent.SAServiceName)
		err = agent.SAConsulLock.Unlock()
		if err != nil {
			logger.Error("failed to release the leadership lock",
				     "error", err, "service", agent.SAServiceName)
		}
		return false
	case <-exitChannel:
		logger.Info("exiting while holding the leadership lock",
			    "service", agent.SAServiceName)
		err = agent.SAConsulLock.Unlock()
		if err != nil {
			logger.Error("failed to release the leadership lock",
				     "error", err, "service", agent.SAServiceName)
		}
		return true
	}
}

func (agent *ServiceAgent) SAPlanRun(logger log.Logger,
				     waitGroup *sync.WaitGroup,
				     plan *watch.Plan) {
	waitGroup.Add(1)
	defer waitGroup.Done()

	err := plan.RunWithClientAndHclog(agent.SAConsulClient, logger)
	if  err != nil {
		logger.Error("failed to run plan", "error", err, "service",
			     agent.SAServiceName)
	}
}

// Get lock and start the service
func (agent *ServiceAgent) SAMonitorService(logger log.Logger,
					    waitGroup *sync.WaitGroup,
					    exitChannel <-chan struct{}) {
	waitGroup.Add(1)
	defer waitGroup.Done()

	params := make(map[string]interface{})

	params["type"] = "key"
	params["key"] = agent.SAConfigKey

	plan, err := watch.Parse(params)
	if err != nil {
		logger.Error("failed to parse param of watch", "error", err,
			     "service", agent.SAServiceName)
		return
	}

	newConf := agent.SARuntimeConfig
	plan.Handler = func(idx uint64, raw interface{}) {
		var value *api.KVPair

		if raw == nil { // nil is a valid return value
			value = nil
		} else {
			var ok bool
			if value, ok = raw.(*api.KVPair); !ok {
				return // ignore
			}

			err = yaml.Unmarshal(value.Value, &newConf)
			if err != nil {
				logger.Error("failed to unmarshal", "error", err,
					     "service", agent.SAServiceName)
				return // ignore
			}
			agent.SARuntimeConfig = newConf
			logger.Info("change of config", "enable autostart",
				    newConf.RCAutostartEnabled, "service",
				    agent.SAServiceName)
		}
	}

	go agent.SAPlanRun(logger, waitGroup, plan)

	for {
		exiting := agent.SAMonitorServiceOnce(logger, waitGroup, exitChannel)
		if exiting {
			plan.Stop()
			return
		}
	}
}

type HostStatus string
const (
	HSUnknown HostStatus = "unknown"
	HSStartFailed HostStatus = "start failed"
	HSStarted HostStatus = "started"
)

type HostAgent struct {
	// Host to monitor
	HASSHHost SSHHost
	// Client to connect to Consul
	HAConsulClient *api.Client
	HASessionTTL time.Duration
	HASessionTTLSeconds int
	HAUuid string
	HAConsulLock *api.Lock
	HALockKey string
	HAConfigKey string
	HARuntimeConfig RuntimeConfig
	HAHostStatus HostStatus
}

func CreateLustreHostAgents(logger log.Logger,
			    consulConf *api.Config,
			    config *ClownfishConfig) ([]*HostAgent, error) {
	hostname, err := os.Hostname()
	if err != nil {
		logger.Error("failed to get local hostname", "error", err)
		return nil, err
	}

	var hostList SSHHostList
	number := 0
	foundMyself := false
	for _, sshHost := range config.CCSSHHosts {
		if (!sshHost.SSHStandalone) || sshHost.SSHHostName == hostname {
			hostList = append(hostList, sshHost)
			if sshHost.SSHHostName == hostname {
				foundMyself = true
			}
			number++
		}
	}

	if !foundMyself {
		return nil, fmt.Errorf("failed to find local host name in the Clownfish config: %s",
				       hostname)
	}

	sort.Sort(hostList)
	foundMyself = false
	var monitorList SSHHostList
	number = 0
	// Add the hosts after localhost to the monitor list
	for _, sshHost := range hostList {
		if sshHost.SSHHostName == hostname {
			foundMyself = true
			continue
		}
		if foundMyself {
			monitorList = append(monitorList, sshHost)
			number++
			if number >= CLF_MAX_WATCH_HOST {
				break
			}
		}
	}

	// This is a loop, add the first few hosts to monitor list
	if number < CLF_MAX_WATCH_HOST {
		for _, sshHost := range hostList {
			if sshHost.SSHHostName == hostname {
				break
			}
			monitorList = append(monitorList, sshHost)
			number++
			if number >= CLF_MAX_WATCH_HOST {
				break
			}
		}
	}

	sort.Sort(monitorList)

	var agents []*HostAgent
	for _, sshHost := range monitorList {
		hostname := sshHost.SSHHostName

		client, err := api.NewClient(consulConf)
		if err != nil {
			logger.Error("failed to create Consul client",
				     "error", err,
				     "hostname", hostname)
			return nil, err
		}

		uuid, err := uuid.GenerateUUID()
		if err != nil {
			logger.Error("failed to generate uuid", "error", err,
				     "hostname", hostname)
			return nil, err
		}

		sessionTTL := fmt.Sprintf("%ds", SessionTTLSeconds)
		waitTime := SessionTTLSeconds * time.Second

		lockKey := CLF_CONSUL_HOST_PATH + "/" + hostname + "/" + CLF_CONSUL_LOCK_KEY
		opts := &api.LockOptions{
			Key:            lockKey,
			Value:          []byte(uuid),
			SessionName:    "Clownfish host Lock",
			MonitorRetries: 5,
			SessionTTL:     sessionTTL,
		}
		//opts.LockWaitTime = waitTime

		lock, err := client.LockOpts(opts)
		if err != nil {
			logger.Error("failed to create lock", "error", err,
				     "hostname", hostname)
			return nil, err
		}

		configKey := CLF_CONSUL_HOST_PATH + "/" + hostname + "/" + CLF_CONSUL_CONFIG_KEY
		runtimeConf := RuntimeConfig {
			RCAutostartEnabled: false,
		}

		agent := HostAgent {
			HASSHHost: sshHost,
			HASessionTTL: waitTime,
			HASessionTTLSeconds: SessionTTLSeconds,
			HALockKey: lockKey,
			HAConsulClient: client,
			HAUuid: uuid,
			HAConsulLock: lock,
			HAConfigKey: configKey,
			HARuntimeConfig: runtimeConf,
		}
		agents = append(agents, &agent)
	}
	return agents, nil
}

// Read the lock value if lock is held. Return error if lock is not held.
func (agent *HostAgent) HAGetLockValue(path string) (bool, string, error) {
	client := agent.HAConsulClient

        kv := client.KV()
	queryOptions := &api.QueryOptions {
		RequireConsistent: true,
	}

        pair, _, err := kv.Get(path, queryOptions)
        if err != nil {
                return false, "", err
        }

        if pair == nil {
                return false, "", nil
        }

        held := pair.Session != ""
        value := string(pair.Value)
        return held, value, nil
}

func (agent *HostAgent) HAGetLeaderUUID() (string, error) {
	held, leaderUUID, err := agent.HAGetLockValue(agent.HALockKey)
	if err != nil {
		return "", err
	}

	if !held {
		return "", fmt.Errorf("failed to get readlock")
	}

	return leaderUUID, nil
}

func (agent *HostAgent) HAGetLeaderUUIDWait(exitChannel <-chan struct{}) (string, bool) {
	for i := 0; i < agent.HASessionTTLSeconds; i++ {
		leaderUUID, err := agent.HAGetLeaderUUID()
		if err == nil {
			return leaderUUID, false
		}

		select {
		case <-time.After(1 * time.Second):
			continue
		case <-exitChannel:
			return "", true
		}
	}
	return "", false
}

// SAAcquireLock blocks until the lock is acquired, returning the leaderLostCh
func (agent *HostAgent) HAAcquireLock(logger log.Logger,
				      exitChannel <-chan struct{}) (<-chan struct{}) {
	for {
		// Attempt lock acquisition
		leaderLostCh, err := agent.HAConsulLock.Lock(exitChannel)
		if err == nil {
			return leaderLostCh
		}

		logger.Info("failed to acquire lock", "error", err)

		// Retry the acquisition
		select {
		case <-time.After(time.Duration(agent.HASessionTTLSeconds) * time.Second):
		case <-exitChannel:
			return nil
		}
	}
}

func (agent *HostAgent) HAMaintainHost(logger log.Logger,
				       waitGroup *sync.WaitGroup,
				       exitChannel <-chan struct{}) {
	waitGroup.Add(1)
	defer waitGroup.Done()

	hostname := agent.HASSHHost.SSHHostName
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	status := HSUnknown
	agent.HAHostStatus = status
	var oldAutostartEnabled bool
	var autostartEnabled bool
	var newStatus HostStatus
	first := true
	for {
		autostartEnabled = agent.HARuntimeConfig.RCAutostartEnabled
		if first || oldAutostartEnabled != autostartEnabled {
			if autostartEnabled {
				logger.Info("auotostart is enabled", "hostname", hostname)
			} else {
				logger.Info("auotostart is disabled", "hostname", hostname)
			}
			first = false
		}
		oldAutostartEnabled = autostartEnabled
		if autostartEnabled {
			if status == HSUnknown || status == HSStartFailed {
				logger.Info("starting host", "hostname", hostname)
			}
			cmd := exec.Command("clownf", "host", "start", hostname)
			stdout.Reset()
			stderr.Reset()
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			startTime := time.Now()
			err := cmd.Run()
			duration := int(time.Since(startTime).Seconds())
			stdoutString := stdout.String()
			stdoutString = strings.Replace(stdoutString, "\n", "\\n", -1)
			stderrString := stderr.String()
			stderrString = strings.Replace(stderrString, "\n", "\\n", -1)

			if err != nil {
				logger.Error("failed to start host",
					     "hostname", hostname,
					     "error", err,
					     "stdout", stdoutString,
					     "stderr", stderrString,
					     "duration", duration)
				newStatus = HSStartFailed
			} else {
				newStatus = HSStarted
			}

			if status != newStatus {
				logger.Info("status change of host",
					    "hostname", hostname, "old",
					    status, "new", newStatus,
					    "stdout", stdoutString,
					    "stderr", stderrString,
					    "duration", duration)
				status = newStatus
				agent.HAHostStatus = newStatus
			} else if (newStatus == HSStarted &&
				   stdout.String() != CLF_MSG_ALREADY_STARTED) {
				logger.Info("started host with stale status of up",
					    "hostname", hostname,
					    "stdout", stdoutString,
					    "stderr", stderrString,
					    "duration", duration)
			}
		}

		select {
		case <-time.After(agent.HASessionTTL):
		case <-exitChannel:
			logger.Error("exiting from maintaining host",
				     "hostname", hostname)
			return
		}
	}
}

func (agent *HostAgent) HAMonitorHostOnce(logger log.Logger,
					  waitGroup *sync.WaitGroup,
					  exitChannel <-chan struct{}) (bool) {
	var err error
	currentLeader, exiting := agent.HAGetLeaderUUIDWait(exitChannel)

	hostname := agent.HASSHHost.SSHHostName
	if exiting {
		logger.Info("exiting when trying to get the leader uuid",
			    "hostname", hostname)
		return true
	}

	// If the leader is not me, cleanup.
	// Note that the currentLeader could be empty string
	if currentLeader != agent.HAUuid {
		if currentLeader == "" {
			logger.Info("not able to get the current leader for a long time",
				    "hostname", hostname)
		} else {
			logger.Info("the current leader is someone else",
				    "hostname", hostname)
		}
	}

	logger.Info("trying to get the leadership lock",
		    "hostname", hostname)
	leaderLostCh := agent.HAAcquireLock(logger, exitChannel)
	if leaderLostCh == nil {
		logger.Info("exiting when trying to get the leadership lock",
			    "hostname", hostname)
		return true
	}

	logger.Info("got the leadership lock", "hostname", hostname)
	go agent.HAMaintainHost(logger, waitGroup, exitChannel)

	// Monitor a loss of leadership
	select {
	case <-leaderLostCh:
		logger.Info("lost the leadership",
			    "hostname", hostname)
		err = agent.HAConsulLock.Unlock()
		if err != nil {
			logger.Error("failed to release the leadership lock",
				     "error", err, "hostname", hostname)
		}
		return false
	case <-exitChannel:
		logger.Info("exiting while holding the leadership lock",
			    "hostname", hostname)
		err = agent.HAConsulLock.Unlock()
		if err != nil {
			logger.Error("failed to release the leadership lock",
				     "error", err, "hostname", hostname)
		}
		return true
	}
}

func (agent *HostAgent) HAPlanRun(logger log.Logger,
				  waitGroup *sync.WaitGroup,
				  plan *watch.Plan) {
	waitGroup.Add(1)
	defer waitGroup.Done()

	hostname := agent.HASSHHost.SSHHostName
	err := plan.RunWithClientAndHclog(agent.HAConsulClient, logger)
	if  err != nil {
		logger.Error("failed to run plan", "error", err, "hostname",
			     hostname)
	}
}

// Get lock and start the service
func (agent *HostAgent) HAMonitorHost(logger log.Logger,
				      waitGroup *sync.WaitGroup,
				      exitChannel <-chan struct{}) {
	hostname := agent.HASSHHost.SSHHostName
	params := make(map[string]interface{})

	params["type"] = "key"
	params["key"] = agent.HAConfigKey

	plan, err := watch.Parse(params)
	if err != nil {
		logger.Error("failed to parse param of watch", "error", err,
			     "hostname", hostname)
		return
	}

	newConf := agent.HARuntimeConfig
	plan.Handler = func(idx uint64, raw interface{}) {
		var value *api.KVPair

		if raw == nil { // nil is a valid return value
			value = nil
		} else {
			var ok bool
			if value, ok = raw.(*api.KVPair); !ok {
				return // ignore
			}

			err = yaml.Unmarshal(value.Value, &newConf)
			if err != nil {
				logger.Error("failed to unmarshal", "error", err,
					     "hostname", hostname)
				return // ignore
			}
			agent.HARuntimeConfig = newConf
			logger.Info("change of config", "enable autostart",
				    newConf.RCAutostartEnabled, "hostname",
				    hostname)
		}
	}

	go agent.HAPlanRun(logger, waitGroup, plan)

	for {
		exiting := agent.HAMonitorHostOnce(logger, waitGroup, exitChannel)
		if exiting {
			plan.Stop()
			return
		}
	}
}

func CreateServiceAgents(logger log.Logger,
			 consulConf *api.Config,
			 clownfishConfig *ClownfishConfig) ([]*ServiceAgent, error) {
	localService, err := filterLocalServices(logger, clownfishConfig)
	if err != nil {
		logger.Error("failed to filter local services")
		return nil, err
	}
	logger.Debug("got local service", "service", localService)

	var agents []*ServiceAgent
	for _, serviceInstance := range localService.LSServiceInstances {
		service := serviceInstance.LSService
		serviceName := service.LSServiceName

		client, err := api.NewClient(consulConf)
		if err != nil {
			logger.Error("failed to create Consul client",
				     "error", err, "service", serviceName)
			return nil, err
		}

		uuid, err := uuid.GenerateUUID()
		if err != nil {
			logger.Error("failed to generate uuid", "error", err,
				     "service", serviceName)
			return nil, err
		}

		sessionTTL := fmt.Sprintf("%ds", SessionTTLSeconds)
		waitTime := SessionTTLSeconds * time.Second

		lockKey := CLF_CONSUL_SERVICE_PATH + "/" + serviceName + "/" + CLF_CONSUL_LOCK_KEY
		opts := &api.LockOptions{
			Key:            lockKey,
			Value:          []byte(uuid),
			SessionName:    "Clownfish service Lock",
			MonitorRetries: 5,
			SessionTTL:     sessionTTL,
		}
		//opts.LockWaitTime = waitTime

		lock, err := client.LockOpts(opts)
		if err != nil {
			logger.Error("failed to create lock", "error", err,
				     "service", serviceName)
			return nil, err
		}

		configKey := CLF_CONSUL_SERVICE_PATH + "/" + serviceName + "/" + CLF_CONSUL_CONFIG_KEY
		runtimeConf := RuntimeConfig {
			RCAutostartEnabled: false,
		}
		agent := ServiceAgent {
			SAServiceStatus: SSUnknown,
			SAServiceName: serviceName,
			SASessionTTL: waitTime,
			SASessionTTLSeconds: SessionTTLSeconds,
			SALockKey: lockKey,
			SAConfigKey: configKey,
			SAConsulClient: client,
			SAUuid: uuid,
			SAConsulLock: lock,
			SARuntimeConfig: runtimeConf,
		}
		agents = append(agents, &agent)
	}
	return agents, nil
}


func VersionCheckOnce(logger log.Logger) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	cmd := exec.Command("clownf", "version_check", "--no_log_prefix")
	stdout.Reset()
	stderr.Reset()
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	startTime := time.Now()
	err := cmd.Run()
	duration := int(time.Since(startTime).Seconds())
	stdoutString := stdout.String()
	stderrString := stderr.String()
	logger.Debug("finished version check", "error", err, "duration",
		     duration)
	if stdoutString != "" {
		logger.Info(stdoutString)
	}
	if stderrString != "" {
		logger.Error(stderrString)
	}
}


// RandomStaggerQuarter returns an interval that is between 3/4 and 5/4 of
// the given interval. The expected value is the interval.
func RandomStaggerQuarter(interval time.Duration) time.Duration {
        stagger := time.Duration(rand.Int63()) % (interval / 2)
        return 3 * (interval / 4 ) + stagger
}

// RandomStagger returns an interval between 0 and the duration
func RandomStagger(intv time.Duration) time.Duration {
        if intv == 0 {
                return 0
        }
        return time.Duration(uint64(rand.Int63()) % uint64(intv))
}


func VersionCheck(logger log.Logger, waitGroup *sync.WaitGroup,
		  exitChannel <-chan struct{}) {
	interval := 24 * time.Hour

	// Do an immediate check within the next 30 seconds
	time.Sleep(RandomStagger(30 * time.Second))
	VersionCheckOnce(logger)

	waitGroup.Add(1)
	defer waitGroup.Done()
	for {
		select {
		case <-time.After(RandomStaggerQuarter(interval)):
			VersionCheckOnce(logger)
		case <-exitChannel:
			logger.Error("exiting from version checking")
			return
		}
	}
}

func main() {
	addr := "127.0.0.1:8500"
	logger := log.New(&log.LoggerOptions{
		Name: "clownf_agent",
	})

	clownfishConfig, err := loadConfig(logger)
	if err != nil {
		logger.Error("failed to load config of Clownfish")
		return
	}

	consulConf := api.DefaultConfig()
	consulConf.Transport.MaxIdleConnsPerHost = 1
	consulConf.Address = addr

	if logger.IsDebug() {
		logger.Debug("config address set", "address", addr)
	}

	// Copied from the Consul API module; set the Scheme based on
	// the protocol field if address looks ike a URL.
	// This can enable the TLS configuration below.
	parts := strings.SplitN(addr, "://", 2)
	if len(parts) == 2 {
		if parts[0] == "http" || parts[0] == "https" {
			consulConf.Scheme = parts[0]
			consulConf.Address = parts[1]
			if logger.IsDebug() {
				logger.Debug("config address parsed", "scheme", parts[0])
				logger.Debug("config scheme parsed", "address", parts[1])
			}
		} // allow "unix:" or whatever else consul supports in the future
	}

	agents, err := CreateServiceAgents(logger, consulConf, clownfishConfig)
	if err != nil {
		logger.Error("failed to create service agents", "error", err)
		return
	}

	hostAgents, err := CreateLustreHostAgents(logger, consulConf, clownfishConfig)
	if err != nil {
		logger.Error("failed to create server agents", "error", err)
		return
	}

	exitChannel := make(chan struct{})
	waitGroup := sync.WaitGroup{}

	go VersionCheck(logger, &waitGroup, exitChannel)

	for _, agent := range agents {
		logger.Info("starting agent for Lustre service",
			    "service_name", agent.SAServiceName)
		go agent.SAMonitorService(logger, &waitGroup, exitChannel)
	}

	for _, hostAgent := range hostAgents {
		logger.Info("starting agent for host", "hostname",
			    hostAgent.HASSHHost.SSHHostName)
		go hostAgent.HAMonitorHost(logger, &waitGroup, exitChannel)
	}

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel)

	for {
		signal := <-signalChannel

		if signal == syscall.SIGINT || signal == syscall.SIGTERM {
			logger.Warn("quiting because of signal", "signal", signal)
			break
		}
	}

	close(exitChannel)
	waitGroup.Wait()
}
