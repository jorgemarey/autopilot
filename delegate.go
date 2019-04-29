package autopilot

import (
	"fmt"
	"log"

	"github.com/hashicorp/consul/agent/consul/autopilot"
	"github.com/hashicorp/raft"
)

// AdvancedAutopilotDelegate is a delegate for autopilot operations.
// This can check zones, versions and non voting servers
type AdvancedAutopilotDelegate struct {
	autopilot.Delegate
	logger *log.Logger
}

// New returns a new AutopilotDelegate using the provided and with improved promotion
// features
func New(logger *log.Logger, delegate autopilot.Delegate) *AdvancedAutopilotDelegate {
	return &AdvancedAutopilotDelegate{
		Delegate: delegate,
		logger:   logger,
	}
}

// PromoteNonVoters implements autopilot.Delegate.PromoteNonVoters
func (d *AdvancedAutopilotDelegate) PromoteNonVoters(conf *autopilot.Config, health autopilot.OperatorHealthReply) ([]raft.Server, error) {
	future := d.Raft().GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("failed to get raft configuration: %v", err)
	}

	// Find any non-voters eligible for promotion.
	servers := future.Configuration().Servers
	stable := autopilot.PromoteStableServers(conf, health, servers)
	// If we have no stable servers we just stop
	if len(stable) == 0 {
		return stable, nil
	}

	info := d.buildServerInfo(conf, servers)
	peers := autopilot.NumPeers(future.Configuration())
	promoted, serverID := PromoteServers(conf, stable, info, peers)
	if serverID != nil { // if we need to demote a server do it now
		futureErr := d.Raft().DemoteVoter(*serverID, 0, 0)
		if err := futureErr.Error(); err != nil {
			return nil, fmt.Errorf("error demoting server %s: %s", "", err)
		}
	}
	return promoted, nil
}

// buildServerInfo returns information about all the servers present in the raft cluster
func (d *AdvancedAutopilotDelegate) buildServerInfo(conf *autopilot.Config, servers []raft.Server) map[raft.ServerID]*serverInfo {
	info := make(map[raft.ServerID]*serverInfo)
	temp := make(map[raft.ServerID]*serverInfo)

	for _, m := range d.Serf().Members() { // get the info for every server
		server, err := getServerInfo(conf.RedundancyZoneTag, conf.UpgradeVersionTag, m, d.IsServer)
		if err == nil && server != nil { // add servers found
			temp[raft.ServerID(server.ID)] = server
		}
	}
	// check if servers exists on the raft list and add suffrage info
	for _, rs := range servers {
		if server, ok := temp[rs.ID]; ok {
			server.RaftStatus = rs.Suffrage
			info[rs.ID] = server
		}
	}
	return info
}
