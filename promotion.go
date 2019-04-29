package autopilot

import (
	"fmt"

	"github.com/hashicorp/consul/agent/consul/autopilot"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

// PromoteServers is an advanced policy that has zones and versions.
// In servers should come all those servers that are not voting at the moment but are stable
func PromoteServers(config *autopilot.Config, servers []raft.Server, info map[raft.ServerID]*serverInfo, peers int) ([]raft.Server, *raft.ServerID) {
	promoted := filterNonVoting(servers, info)
	// return if nothing else to do
	if (config.RedundancyZoneTag == "" && config.DisableUpgradeMigration) || len(promoted) == 0 {
		return promoted, nil
	}

	// Check if we have to perform upgrade
	// THINK: if we have a failing server avoid trying migration
	if !config.DisableUpgradeMigration {
		servers, id, moveOn := filterByVersion(config, promoted, info, peers)
		if !moveOn {
			return servers, id
		}
	}

	// Filter by zone
	if config.RedundancyZoneTag != "" {
		promoted = filterByZone(config, promoted, info)
	}
	return promoted, nil
}

// filterNonVoting iterates over the provided servers and only returns those that are
// not declared as non voting
func filterNonVoting(servers []raft.Server, info map[raft.ServerID]*serverInfo) []raft.Server {
	var promoted []raft.Server
	for _, server := range servers {
		if sinfo, ok := info[server.ID]; ok && sinfo.Voting {
			promoted = append(promoted, server)
		}
	}
	return promoted
}

// filterByVersion check the different versions of servers in the cluster
// and returns a server to promote or demote taking into account the
// cluster status. Also returns if we can continue or we should stop
func filterByVersion(config *autopilot.Config, servers []raft.Server, info map[raft.ServerID]*serverInfo, peers int) ([]raft.Server, *raft.ServerID, bool) {
	versions := getVersions(info)
	checkZone := config.RedundancyZoneTag != ""
	switch len(versions) {
	case 1: // nothing to do
		return nil, nil, true
	case 2:
		upgradeInfo := getUpgradeInfo(servers, versions, info, checkZone, peers)
		if upgradeInfo.upgraded {
			return nil, nil, false
		}
		if upgradeInfo.canUpgrade {
			fmt.Println(upgradeInfo.duringUpgrade, peers%2)
			if upgradeInfo.duringUpgrade && peers%2 == 0 {
				id := pickServerToDemote(upgradeInfo.lowerVoters, info, checkZone)
				return nil, &id, false
			}
			server := pickNewServer(servers, info, upgradeInfo.higherVoters, upgradeInfo.highVersion, checkZone)
			return server, nil, false
		}
		fallthrough
	default: // more than 2
		return nil, nil, false
		// THINK: we could do the case of 2 or more.
		// The logic would be to select the higher version servers vs old others
	}
}

// filterByZone only returns a server for each zone whiout a voter (or any server without zone)
// No check on version is performed, so if we do this, it means that we only have servers of one version
func filterByZone(config *autopilot.Config, servers []raft.Server, info map[raft.ServerID]*serverInfo) []raft.Server {
	zoneVoter := make(map[string]bool)
	for _, server := range info { // we set if there're a voter en every zone we know
		zone := server.Zone
		zoneVoter[zone] = zoneVoter[zone] || (autopilot.IsPotentialVoter(server.RaftStatus) && server.Status == serf.StatusAlive)
	}
	var promoted []raft.Server
	// THINK: if we have several versions and we are upgraded we need to add a new server version, if
	// we aren't upgraded yet, use an older version (if versions upgraded are enabled)
	for _, server := range servers {
		sinfo, ok := info[server.ID]
		if ok && (sinfo.Zone == "" || !zoneVoter[sinfo.Zone]) { // If no zone or zone doesn't have a server
			promoted = append(promoted, server)
			zoneVoter[sinfo.Zone] = true // we set that we already got a server on that zone
		}
	}
	return promoted
}
