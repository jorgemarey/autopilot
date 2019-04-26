package autopilot

import (
	"github.com/hashicorp/consul/agent/consul/autopilot"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

// PromoteServers is an advanced policy that has zones and versions.
// In servers should come all those servers that are not voting at the moment but are stable
func PromoteServers(autopilotConfig *autopilot.Config, servers []raft.Server, info map[raft.ServerID]*serverInfo, peers int) ([]raft.Server, *raft.ServerID) {
	promoted := filterNonVoting(servers, info)

	if (autopilotConfig.RedundancyZoneTag == "" && autopilotConfig.DisableUpgradeMigration) || len(promoted) == 0 { // return if nothing else to do
		return promoted, nil
	}

	// Check if we have to perform upgrade
	// THINK: if we have a failing server avoid trying migration
	if !autopilotConfig.DisableUpgradeMigration {
		servers, id := filterVersions(promoted, info, peers)
		if id != nil || servers != nil {
			return servers, id
		}
	}

	// Filter by zone
	if autopilotConfig.RedundancyZoneTag != "" {
		promoted = filterZoneServers(promoted, info)
	}
	return promoted, nil
}

func filterNonVoting(servers []raft.Server, info map[raft.ServerID]*serverInfo) []raft.Server {
	var promoted []raft.Server
	for _, server := range servers {
		if sinfo, ok := info[server.ID]; ok && sinfo.Voting {
			promoted = append(promoted, server)
		}
	}
	return promoted
}

func filterVersions(servers []raft.Server, info map[raft.ServerID]*serverInfo, peers int) ([]raft.Server, *raft.ServerID) {
	_, versions := makeInfoMaps(info)
	switch len(versions) {
	case 1: // nothing to do
	case 2:
		// 1) if there no voters in the lower version servers we stop
		// 2) if we have enought servers to replace old ones we procede
		// 3) if current servers has at least one of the new version -> duringUpgrade = true
		// 3.1 ) if duringUpgrade and peers%2 == 0 demote peer
		// 3.2 ) select one server to promote from those left from the last filter (not on an upgraded zone)
		higherServers, lowerVoters, canUpgrade, duringUpgrade, upgraded := getUpgradeInfo(servers, versions, info)
		if upgraded {
			return nil, nil
		}
		if canUpgrade {
			if duringUpgrade && peers%2 == 0 {
				id := raft.ServerID(lowerVoters[0].ID)
				// TODO: Pick one of the lowerVoters (take into account the zone if enabled)
				return nil, &id
			}
			// TODO: pick one of the higherServers (take into account the zone if enabled)
			for _, server := range servers {
				if server.ID == raft.ServerID(higherServers[0].ID) {
					return []raft.Server{server}, nil
				}
			}
		}
	default: // more than 2
		// THINK: we could do the case of 2 or more.
		// The logic would be to select the higher version servers vs old others
	}
	return nil, nil
}

// If we do this, it means that we only have servers of one version
func filterZoneServers(servers []raft.Server, info map[raft.ServerID]*serverInfo) []raft.Server {
	zoneVoter := make(map[string]bool)
	for _, server := range info { // we set if there're a voter en every zone we know
		zone := server.Zone
		zoneVoter[zone] = zoneVoter[zone] || (autopilot.IsPotentialVoter(server.RaftStatus) && server.Status == serf.StatusAlive)
	}
	promoted := make([]raft.Server, 0)
	for _, server := range servers {
		sinfo, ok := info[server.ID]
		if ok && (sinfo.Zone == "" || !zoneVoter[sinfo.Zone]) { // If no zone or zone doesn't have a server
			promoted = append(promoted, server)
			zoneVoter[sinfo.Zone] = true // we set that we already got a server on that zone
		}
	}
	return promoted
}
