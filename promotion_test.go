package autopilot

import (
	"testing"
	"time"

	"github.com/hashicorp/consul/agent/consul/autopilot"
	"github.com/hashicorp/go-version"
	"github.com/hashicorp/serf/serf"

	"github.com/hashicorp/raft"
	"github.com/pascaldekloe/goe/verify"
)

func TestFilterNonVotingServers(t *testing.T) {
	cases := []struct {
		name       string
		servers    []raft.Server
		info       map[raft.ServerID]*serverInfo
		promotions []raft.Server
	}{
		{
			name: "one non voting server, no promotions",
			servers: []raft.Server{
				{ID: "a", Suffrage: raft.Voter},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{Voting: false},
			},
			promotions: []raft.Server{},
		},
		{
			name: "one voting server, one promotion",
			servers: []raft.Server{
				{ID: "a"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{Voting: true},
			},
			promotions: []raft.Server{
				{ID: "a"},
			},
		},
		{
			name: "one server, no info, no promotion",
			servers: []raft.Server{
				{ID: "a"},
			},
			info:       map[raft.ServerID]*serverInfo{},
			promotions: []raft.Server{},
		},
		{
			name: "two voting server, one not, two promotions",
			servers: []raft.Server{
				{ID: "a"},
				{ID: "b"},
				{ID: "c"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{Voting: true},
				"b": &serverInfo{Voting: false},
				"c": &serverInfo{Voting: true},
			},
			promotions: []raft.Server{
				{ID: "a"},
				{ID: "c"},
			},
		},
	}
	for _, tc := range cases {
		promotions := filterNonVoting(tc.servers, tc.info)
		verify.Values(t, tc.name, promotions, tc.promotions)
	}
}

func TestFilterByZoneServers(t *testing.T) {
	cases := []struct {
		name       string
		servers    []raft.Server
		info       map[raft.ServerID]*serverInfo
		promotions []raft.Server
	}{
		{
			name: "voter in all zones, no promotions",
			servers: []raft.Server{
				{ID: "a"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{Zone: "1", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
				"b": &serverInfo{Zone: "1", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
			},
			promotions: []raft.Server{},
		},
		{
			name: "voter in non existent zone, one promotion",
			servers: []raft.Server{
				{ID: "a"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{Zone: "1", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
				"b": &serverInfo{Zone: "2", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
			},
			promotions: []raft.Server{
				{ID: "a"},
			},
		},
		{
			name: "voter zone with failing server, one promotion",
			servers: []raft.Server{
				{ID: "a"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{Zone: "1", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
				"b": &serverInfo{Zone: "1", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusFailed}},
			},
			promotions: []raft.Server{
				{ID: "a"},
			},
		},
		{
			name: "voter zone with failing no voter, one promotion",
			servers: []raft.Server{
				{ID: "a"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{Zone: "1", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
				"b": &serverInfo{Zone: "1", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusLeft}},
			},
			promotions: []raft.Server{
				{ID: "a"},
			},
		},
		{
			name: "voter without zone, one promotion",
			servers: []raft.Server{
				{ID: "a"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{Zone: "", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
				"b": &serverInfo{Zone: "1", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
				"c": &serverInfo{Zone: "2", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
			},
			promotions: []raft.Server{
				{ID: "a"},
			},
		},
		{
			name: "non voter in zone, one promotion",
			servers: []raft.Server{
				{ID: "a"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{Zone: "1", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
				"b": &serverInfo{Zone: "1", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusFailed}},
				"c": &serverInfo{Zone: "2", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
				"d": &serverInfo{Zone: "3", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
			},
			promotions: []raft.Server{
				{ID: "a"},
			},
		},
		{
			name: "non voter in zone, one promotion, multiple non voters",
			servers: []raft.Server{
				{ID: "a"}, {ID: "e"}, {ID: "f"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{Zone: "1", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
				"b": &serverInfo{Zone: "1", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusFailed}},
				"c": &serverInfo{Zone: "2", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
				"f": &serverInfo{Zone: "2", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
				"d": &serverInfo{Zone: "3", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
				"e": &serverInfo{Zone: "3", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
			},
			promotions: []raft.Server{
				{ID: "a"},
			},
		},
		{
			name: "non voter in zone, one promotion, multiple non voters in that zone",
			servers: []raft.Server{
				{ID: "a"}, {ID: "e"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{Zone: "1", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
				"b": &serverInfo{Zone: "1", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusFailed}},
				"e": &serverInfo{Zone: "1", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusFailed}},
				"c": &serverInfo{Zone: "2", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
				"d": &serverInfo{Zone: "3", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive}},
			},
			promotions: []raft.Server{
				{ID: "a"},
			},
		},
	}
	for _, tc := range cases {
		promotions := filterZoneServers(tc.servers, tc.info)
		verify.Values(t, tc.name, promotions, tc.promotions)
	}
}

func TestFilterVersions(t *testing.T) {
	v1 := *version.Must(version.NewVersion("v1.0.0"))
	v2 := *version.Must(version.NewVersion("v2.0.0"))
	// v3 := *version.Must(version.NewVersion("v3.0.0"))

	cases := []struct {
		name       string
		peers      int
		servers    []raft.Server
		info       map[raft.ServerID]*serverInfo
		promotions []raft.Server
		serverID   *raft.ServerID
	}{
		{
			name:  "all servers in the same version",
			peers: 3,
			servers: []raft.Server{
				{ID: "a"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "a"}},
				"b": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "b"}},
				"c": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "c"}},
				"d": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "d"}},
			},
			promotions: []raft.Server{},
			serverID:   nil,
		},
		{
			name:  "new server in new version",
			peers: 3,
			servers: []raft.Server{
				{ID: "a"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v2, ID: "a"}},
				"b": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "b"}},
				"c": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "c"}},
				"d": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "d"}},
			},
			promotions: []raft.Server{},
			serverID:   nil,
		},
		{
			name:  "tow new server in new version",
			peers: 3,
			servers: []raft.Server{
				{ID: "a"}, {ID: "e"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v2, ID: "a"}},
				"e": &serverInfo{RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v2, ID: "e"}},
				"b": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "b"}},
				"c": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "c"}},
				"d": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "d"}},
			},
			promotions: []raft.Server{},
			serverID:   nil,
		},
		{
			name:  "three new servers in new version",
			peers: 3,
			servers: []raft.Server{
				{ID: "a"}, {ID: "e"}, {ID: "f"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v2, ID: "a"}},
				"e": &serverInfo{RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v2, ID: "e"}},
				"f": &serverInfo{RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v2, ID: "f"}},
				"b": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "b"}},
				"c": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "c"}},
				"d": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "d"}},
			},
			promotions: []raft.Server{
				{ID: "a"},
			},
			serverID: nil,
		},
		{
			name:  "three new servers in new version one in old",
			peers: 4,
			servers: []raft.Server{
				{ID: "e"}, {ID: "f"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v2, ID: "a"}},
				"e": &serverInfo{RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v2, ID: "e"}},
				"f": &serverInfo{RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v2, ID: "f"}},
				"b": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "b"}},
				"c": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "c"}},
				"d": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "d"}},
			},
			promotions: []raft.Server{},
			serverID:   newID("b"),
		},
		{
			name:  "one new servers in new version, two in old",
			peers: 3,
			servers: []raft.Server{
				{ID: "e"}, {ID: "f"}, {ID: "b"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v2, ID: "a"}},
				"e": &serverInfo{RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v2, ID: "e"}},
				"f": &serverInfo{RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v2, ID: "f"}},
				"b": &serverInfo{RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "b"}},
				"c": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "c"}},
				"d": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "d"}},
			},
			promotions: []raft.Server{
				{ID: "e"},
			},
			serverID: nil,
		},
		{
			name:  "three voters new version (all), one in old",
			peers: 4,
			servers: []raft.Server{
				{ID: "b"}, {ID: "c"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v2, ID: "a"}},
				"e": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v2, ID: "e"}},
				"f": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v2, ID: "f"}},
				"b": &serverInfo{RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "b"}},
				"c": &serverInfo{RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "c"}},
				"d": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "d"}},
			},
			promotions: []raft.Server{},
			serverID:   newID("d"),
		},
		{
			name:  "three voters new version (all), none in old",
			peers: 3,
			servers: []raft.Server{
				{ID: "b"}, {ID: "c"}, {ID: "d"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v2, ID: "a"}},
				"e": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v2, ID: "e"}},
				"f": &serverInfo{RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v2, ID: "f"}},
				"b": &serverInfo{RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "b"}},
				"c": &serverInfo{RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "c"}},
				"d": &serverInfo{RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "d"}},
			},
			promotions: []raft.Server{},
			serverID:   nil,
		},
	}
	for _, tc := range cases {
		promotions, serverID := filterVersions(tc.servers, tc.info, tc.peers)
		verify.Values(t, tc.name, promotions, tc.promotions)
		verify.Values(t, tc.name, serverID, tc.serverID)
	}
}

func TestPromote(t *testing.T) {
	v1 := *version.Must(version.NewVersion("v1.0.0"))
	// v2 := *version.Must(version.NewVersion("v2.0.0"))
	// v3 := *version.Must(version.NewVersion("v3.0.0"))

	config := &autopilot.Config{
		LastContactThreshold:    5 * time.Second,
		MaxTrailingLogs:         100,
		ServerStabilizationTime: 3 * time.Second,
		RedundancyZoneTag:       "ap_zone",
		UpgradeVersionTag:       "ap_version",
	}

	cases := []struct {
		name       string
		peers      int
		servers    []raft.Server
		info       map[raft.ServerID]*serverInfo
		promotions []raft.Server
		serverID   *raft.ServerID
	}{
		{
			name:  "three zones, voters, non voters and two non voting",
			peers: 3,
			servers: []raft.Server{
				{ID: "d"}, {ID: "e"}, {ID: "f"}, {ID: "g"}, {ID: "h"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{Voting: true, Zone: "1", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "a"}},
				"b": &serverInfo{Voting: true, Zone: "2", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "b"}},
				"c": &serverInfo{Voting: true, Zone: "3", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "c"}},
				"d": &serverInfo{Voting: true, Zone: "1", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "d"}},
				"e": &serverInfo{Voting: true, Zone: "2", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "e"}},
				"f": &serverInfo{Voting: true, Zone: "3", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "f"}},
				"g": &serverInfo{Voting: false, Zone: "", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "g"}},
				"h": &serverInfo{Voting: false, Zone: "", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "h"}},
			},
			promotions: []raft.Server{},
			serverID:   nil,
		},
		{
			name:  "3 zones, voters (one failed), 4 non voters and two non voting",
			peers: 3,
			servers: []raft.Server{
				{ID: "d"}, {ID: "e"}, {ID: "f"}, {ID: "g"}, {ID: "h"}, {ID: "i"},
			},
			info: map[raft.ServerID]*serverInfo{
				"a": &serverInfo{Voting: true, Zone: "1", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusFailed, Build: v1, ID: "a"}},
				"b": &serverInfo{Voting: true, Zone: "2", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "b"}},
				"c": &serverInfo{Voting: true, Zone: "3", RaftStatus: raft.Voter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "c"}},
				"d": &serverInfo{Voting: true, Zone: "1", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "d"}},
				"e": &serverInfo{Voting: true, Zone: "2", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "e"}},
				"f": &serverInfo{Voting: true, Zone: "3", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "f"}},
				"i": &serverInfo{Voting: true, Zone: "1", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "i"}},
				"g": &serverInfo{Voting: false, Zone: "", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "g"}},
				"h": &serverInfo{Voting: false, Zone: "", RaftStatus: raft.Nonvoter, ServerInfo: &autopilot.ServerInfo{Status: serf.StatusAlive, Build: v1, ID: "h"}},
			},
			promotions: []raft.Server{
				{ID: "d"},
			},
			serverID: nil,
		},
	}
	for _, tc := range cases {
		promotions, serverID := PromoteServers(config, tc.servers, tc.info, tc.peers)
		verify.Values(t, tc.name, promotions, tc.promotions)
		verify.Values(t, tc.name, serverID, tc.serverID)
	}
}

func newID(idStr string) *raft.ServerID {
	id := raft.ServerID(idStr)
	return &id
}
