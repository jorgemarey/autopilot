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

type upgradeInfo struct {
	highVersion   version.Version
	higherServers []*serverInfo // servers that are not voters (filtered by the provided raft array)
	higherVoters  []*serverInfo
	lowerVoters   []*serverInfo
	canUpgrade    bool
	duringUpgrade bool
	upgraded      bool
}

// This receives the array of possible new promotions, and the map split by version containing
// information about all the servers.
// Returns the high version servers of those possible, the low verion currently voting, and
// three booleans meaning: canUpgrade, duringUpgrade and upgraded
func getUpgradeInfo(servers []raft.Server, versions map[string][]*serverInfo, info map[raft.ServerID]*serverInfo,
	checkZone bool, peers int) *upgradeInfo {
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
	higherServers = make([]*serverInfo, 0) // this filters the servers with those possible to promote
	usefulHighServers := 0
	zones := make(map[string]struct{})
	for _, server := range servers {
		if sinfo, ok := info[server.ID]; ok && sinfo.Build.Equal(higher) {
			higherServers = append(higherServers, sinfo)
			if _, ok := zones[sinfo.Zone]; !checkZone || (checkZone && (!ok || sinfo.Zone == "")) {
				usefulHighServers++
				zones[sinfo.Zone] = struct{}{}
			}
		}
	}
	if peers%2 == 0 {
		usefulHighServers++
	}
	canUpgrade := usefulHighServers >= len(lowerVoters)
	upgraded := !(lowerVoters != nil && len(lowerVoters) > 0)
	return &upgradeInfo{
		highVersion:   *higher,
		higherVoters:  higherVoters,
		higherServers: higherServers,
		lowerVoters:   lowerVoters,
		canUpgrade:    canUpgrade,
		duringUpgrade: (!upgraded) && (higherVoters != nil && len(higherVoters) > 0),
		upgraded:      upgraded,
	}
}

// this picks one of the highers servers
func pickNewServer(servers []raft.Server, info map[raft.ServerID]*serverInfo, voters []*serverInfo, version version.Version, checkZone bool) []raft.Server {
	zones := make(map[string]struct{})
	for _, server := range voters {
		zones[server.Zone] = struct{}{}
	}
	for _, server := range servers {
		if sinfo, ok := info[server.ID]; ok && sinfo.Build.Equal(&version) {
			if _, ok := zones[sinfo.Zone]; !checkZone || (checkZone && (!ok || sinfo.Zone == "")) {
				return []raft.Server{server}
			}
		}
	}
	return nil
}

// THINK: leave the leader to the end
func pickServerToDemote(voters []*serverInfo, info map[raft.ServerID]*serverInfo, checkZone bool) raft.ServerID {
	return raft.ServerID(voters[0].ID)
}

func getVersions(info map[raft.ServerID]*serverInfo) map[string][]*serverInfo {
	versions := make(map[string][]*serverInfo)
	for _, server := range info {
		if server.Voting { // we only take into account voting servers
			build := server.Build.String()
			version, ok := versions[build]
			if !ok {
				version = make([]*serverInfo, 0)
			}
			versions[build] = append(version, server)
		}
	}
	return versions
}
