package autopilot

import (
	"github.com/hashicorp/consul/agent/consul/autopilot"
	"github.com/hashicorp/go-version"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

const baseVersion = "v0.0.1"

type serverInfo struct {
	*autopilot.ServerInfo
	Zone       string
	Voting     bool
	RaftStatus raft.ServerSuffrage
}

func getServerInfo(zoneTag, upgradeTag string, m serf.Member, serverFn func(m serf.Member) (*autopilot.ServerInfo, error)) (*serverInfo, error) {
	info, err := serverFn(m)
	if err != nil || info == nil {
		return nil, err
	}
	si := &serverInfo{ServerInfo: info, Voting: true}
	if v, ok := m.Tags["nonvoter"]; ok && v == "1" {
		si.Voting = false
	}

	if zoneTag != "" {
		si.Zone = m.Tags[zoneTag]
	}
	if upgradeTag != "" {
		si.Build = *version.Must(version.NewVersion(baseVersion))
		buildVersion, err := version.NewVersion(m.Tags[upgradeTag])
		if err == nil {
			si.Build = *buildVersion
		}
	}
	return si, nil
}

// This receives the array of possible new promotions, and the map split by version containing
// information about all the servers.
// Returns the high version servers of those possible, the low verion currently voting, and
// three booleans meaning: canUpgrade, duringUpgrade and upgraded
func getUpgradeInfo(servers []raft.Server, versions map[string][]*serverInfo, info map[raft.ServerID]*serverInfo) ([]*serverInfo, []*serverInfo, bool, bool, bool) {
	higher := version.Must(version.NewVersion(baseVersion))
	var higherServers []*serverInfo
	var lowerServers []*serverInfo
	i := 0
	for k, v := range versions {
		if nv := version.Must(version.NewVersion(k)); nv.GreaterThan(higher) {
			higher = nv
			higherServers = v
		} else {
			lowerServers = v
		}
		if i == 0 {
			lowerServers = v
		}
		i++
	}
	var lowerVoters []*serverInfo
	for _, server := range lowerServers {
		if autopilot.IsPotentialVoter(server.RaftStatus) {
			lowerVoters = append(lowerVoters, server)
		}
	}
	var higherVoters []*serverInfo
	for _, server := range higherServers {
		if autopilot.IsPotentialVoter(server.RaftStatus) {
			higherVoters = append(higherVoters, server)
		}
	}
	higherVoter := (lowerVoters != nil && len(lowerVoters) > 0)
	higherServers = make([]*serverInfo, 0)
	for _, server := range servers {
		if sinfo, ok := info[server.ID]; ok {
			higherServers = append(higherServers, sinfo)
		}
	}
	canUpgrade := len(higherServers)+len(higherVoters) >= len(lowerVoters)
	upgraded := !(lowerVoters != nil && len(lowerVoters) > 0)
	return higherServers, lowerVoters, canUpgrade, (!upgraded) && higherVoter, upgraded
}

func makeInfoMaps(info map[raft.ServerID]*serverInfo) (map[string][]*serverInfo, map[string][]*serverInfo) {
	zones := make(map[string][]*serverInfo)
	versions := make(map[string][]*serverInfo)
	for _, server := range info {
		zone, ok := zones[server.Zone]
		if !ok {
			zone = make([]*serverInfo, 0)
		}
		zones[server.Zone] = append(zone, server)

		build := server.Build.String()
		version, ok := versions[build]
		if !ok {
			version = make([]*serverInfo, 0)
		}
		versions[build] = append(version, server)
	}
	return zones, versions
}
