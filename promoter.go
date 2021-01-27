package autopilot

import (
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-version"
	"github.com/hashicorp/raft"
	ra "github.com/hashicorp/raft-autopilot"
)

const baseVersion = "v0.0.1"

// ImprovedPromoter is a new version of the promoter with improved funcionality
type ImprovedPromoter struct {
	logger hclog.Logger
}

// New will create a new promoter
func New(options ...Option) ra.Promoter {
	p := &ImprovedPromoter{
		logger: hclog.Default().Named("promoter"),
	}
	for _, opt := range options {
		opt(p)
	}
	return p
}

// GetServerExt returns some object that should be stored in the Ext field of the Server
// This value will not be used by the code in this repo but may be used by the other
// Promoter methods and the application utilizing autopilot. If the value returned is
// nil the extended state will not be updated.
func (p *ImprovedPromoter) GetServerExt(config *ra.Config, srvState *ra.ServerState) interface{} {
	extraConfig := config.Ext.(ExtraConfig)

	ext := ExtraServerInfo{}
	if srvState.Server.Ext != nil {
		ext = srvState.Server.Ext.(ExtraServerInfo)
	}
	ext.Zone = string(srvState.Server.ID)
	if zoneTag := extraConfig.RedundancyZoneTag; zoneTag != "" {
		if zone := srvState.Server.Meta[zoneTag]; zone != "" {
			ext.Zone = zone
		}
	}
	ext.Version = srvState.Server.Version
	if uTag := extraConfig.UpgradeVersionTag; uTag != "" {
		version := srvState.Server.Meta[uTag]
		if version == "" {
			version = baseVersion
		}
		ext.Version = version
	}
	p.logger.Debug("Server ext", "id", srvState.Server.ID, "version", ext.Version, "zone", ext.Zone, "nonvoter", ext.NonVoter)
	return ext
}

// GetStateExt returns some object that should be stored in the Ext field of the State
// This value will not be used by the code in this repo but may be used by the other
// Promoter methods and the application utilizing autopilot. If the value returned is
// nil the extended state will not be updated.
func (p *ImprovedPromoter) GetStateExt(config *ra.Config, state *ra.State) interface{} {
	// TODO: this is left untouched
	return nil
}

// GetNodeTypes returns a map of ServerID to NodeType for all the servers which
// should have their NodeType field updated
func (p *ImprovedPromoter) GetNodeTypes(config *ra.Config, state *ra.State) map[raft.ServerID]ra.NodeType {
	// TODO: this is left untouched
	types := make(map[raft.ServerID]ra.NodeType)
	for id := range state.Servers {
		// this basic implementation has all nodes be of the "voter" type regardless of
		// any other settings. That means that in a healthy state all nodes in the cluster
		// will be a voter.
		types[id] = ra.NodeVoter
	}
	return types
}

// CalculatePromotionsAndDemotions return the changes
func (p *ImprovedPromoter) CalculatePromotionsAndDemotions(config *ra.Config, state *ra.State) ra.RaftChanges {
	ableServers := make(map[raft.ServerID]*ra.ServerState)

	// filter only those that are stable and can be voters
	now := time.Now()
	minStableDuration := state.ServerStabilizationTime(config)
	for id, server := range state.Servers {
		// remove nonVoting servers
		extra := server.Server.Ext.(ExtraServerInfo)
		if extra.NonVoter {
			continue
		}

		// ignore staging state as they are not ready yet
		if server.State == ra.RaftNonVoter && server.Health.IsStable(now, minStableDuration) {
			ableServers[id] = server
		}
	}

	// return if nothing else to do
	if len(ableServers) == 0 {
		p.logger.Debug("No raft changes")
		return ra.RaftChanges{}
	}

	extraConfig := config.Ext.(ExtraConfig)

	// Check if we have to perform upgrade
	if !extraConfig.DisableUpgradeMigration {
		changes, canContinue := p.filterByVersion(config, state, ableServers)
		if !canContinue {
			p.logger.Debug("New changes to do", "promotions", changes.Promotions, "demotions", changes.Demotions, "leader", changes.Leader)
			return changes
		}
	}

	// Filter by zone
	if extraConfig.RedundancyZoneTag != "" {
		return p.filterByZone(config, state, ableServers)
	}

	// add these servers so if we don't change anything those need to be promoted
	var changes ra.RaftChanges
	for id := range ableServers {
		changes.Promotions = append(changes.Promotions, id)
	}
	p.logger.Debug("New changes to do", "promotions", changes.Promotions, "demotions", changes.Demotions, "leader", changes.Leader)
	return changes
}

// FilterFailedServerRemovals takes in the current state and structure outlining all the
// failed/stale servers and will return those failed servers which the promoter thinks
// should be allowed to be removed.
func (p *ImprovedPromoter) FilterFailedServerRemovals(config *ra.Config, state *ra.State, failed *ra.FailedServers) *ra.FailedServers {
	// TODO: this is left untouched
	return failed
}

type ExtraServerInfo struct {
	NonVoter bool
	Zone     string
	Version  string
}

type ExtraConfig struct {
	RedundancyZoneTag       string
	DisableUpgradeMigration bool
	UpgradeVersionTag       string
}

func (p *ImprovedPromoter) filterByVersion(config *ra.Config, state *ra.State, filtered map[raft.ServerID]*ra.ServerState) (ra.RaftChanges, bool) {
	versions, hv, lv := getVersionInfo(config, state)
	switch len(versions) {
	case 1: // nothing to do
		return ra.RaftChanges{}, true
	case 2:
		changes := p.performVersionUpgrade(config, state, filtered, hv, lv)
		return changes, false
	default: // more than 2
		return ra.RaftChanges{}, false
		// THINK: we could do the case of 2 or more.
		// The logic would be to select the higher version servers vs old others
	}
}

func (p *ImprovedPromoter) filterByZone(config *ra.Config, state *ra.State, filtered map[raft.ServerID]*ra.ServerState) ra.RaftChanges {
	zoneVoter := make(map[string]struct{})
	for _, srvID := range state.Voters {
		srv := state.Servers[srvID]
		zone := serverZone(srv.Server)
		zoneVoter[zone] = struct{}{}
	}

	var changes ra.RaftChanges
	for id, srv := range filtered {
		zone := serverZone(srv.Server)
		if _, ok := zoneVoter[zone]; !ok {
			changes.Promotions = append(changes.Promotions, id)
			zoneVoter[zone] = struct{}{}
		}
	}
	p.logger.Debug("New changes to do", "promotions", changes.Promotions, "demotions", changes.Demotions, "leader", changes.Leader)
	return changes
}

func (p *ImprovedPromoter) performVersionUpgrade(config *ra.Config, state *ra.State, filtered map[raft.ServerID]*ra.ServerState, hv, lv *version.Version) ra.RaftChanges {
	var changes ra.RaftChanges
	var highVersionVoter, lowVersionVoter, highVersionLeader bool

	for _, id := range state.Voters {
		voter := state.Servers[id]
		serverInfo := voter.Server.Ext.(ExtraServerInfo)
		highVersionVoter = highVersionVoter || version.Must(version.NewVersion(serverInfo.Version)).Equal(hv)
		lowVersionVoter = lowVersionVoter || version.Must(version.NewVersion(serverInfo.Version)).Equal(lv)
	}
	leader := state.Servers[state.Leader]
	serverInfo := leader.Server.Ext.(ExtraServerInfo)
	highVersionLeader = version.Must(version.NewVersion(serverInfo.Version)).Equal(hv)

	// if no voters with the low version exist we're upgraded
	if !lowVersionVoter {
		return changes
	}

	// if no voters with the high version, we check that the number of servers is enought
	if !highVersionVoter {
		usefulHighVersionServers := make([]ra.Server, 0)
		zones := make(map[string]struct{})
		checkZone := config.Ext.(ExtraConfig).RedundancyZoneTag != ""
		for _, srv := range filtered {
			serverInfo := srv.Server.Ext.(ExtraServerInfo)
			if !version.Must(version.NewVersion(serverInfo.Version)).Equal(hv) {
				continue
			}
			if _, ok := zones[serverInfo.Zone]; !checkZone || (checkZone && !ok) {
				usefulHighVersionServers = append(usefulHighVersionServers, srv.Server)
				zones[serverInfo.Zone] = struct{}{}
			}
		}
		if len(usefulHighVersionServers) > len(state.Voters) {
			for _, srv := range usefulHighVersionServers {
				changes.Promotions = append(changes.Promotions, srv.ID)
			}
		}
		return changes
	}

	// If we're here we have servers on both versions as voters, but we need to apply a leadership change
	if !highVersionLeader {
		highVersionVoters := make([]raft.ServerID, 0)
		for _, id := range state.Voters {
			voter := state.Servers[id]
			serverInfo := voter.Server.Ext.(ExtraServerInfo)
			if version.Must(version.NewVersion(serverInfo.Version)).Equal(hv) {
				highVersionVoters = append(highVersionVoters, id)
			}
		}
		ra.SortServers(highVersionVoters, state)
		changes.Leader = highVersionVoters[0]
		return changes
	}

	// we have voters in two versions and a leader in the new version, demote old ones
	for _, id := range state.Voters {
		voter := state.Servers[id]
		serverInfo := voter.Server.Ext.(ExtraServerInfo)
		if version.Must(version.NewVersion(serverInfo.Version)).Equal(lv) {
			changes.Demotions = append(changes.Demotions, id)
		}
	}

	return changes
}
