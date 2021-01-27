package autopilot

import (
	"time"

	"github.com/hashicorp/go-version"
	ra "github.com/hashicorp/raft-autopilot"
)

func serverZone(srv ra.Server) string {
	extra := srv.Ext.(ExtraServerInfo)
	return extra.Zone
}

func getVersionInfo(config *ra.Config, state *ra.State) (map[string][]*ra.ServerState, *version.Version, *version.Version) {
	versions := make(map[string][]*ra.ServerState)
	var higher, lower *version.Version

	now := time.Now()
	minStableDuration := state.ServerStabilizationTime(config)
	for _, srv := range state.Servers {
		if srv.Health.IsStable(now, minStableDuration) {
			extra := srv.Server.Ext.(ExtraServerInfo)
			version, ok := versions[extra.Version]
			if !ok {
				version = make([]*ra.ServerState, 0)
			}
			versions[extra.Version] = append(version, srv)
		}
	}
	first := true
	for k := range versions {
		v := version.Must(version.NewVersion(k))
		if first {
			higher = v
			lower = v
			first = false
			continue
		}
		if v.GreaterThan(higher) {
			higher = v
		}
		if v.LessThan(lower) {
			lower = v
		}
	}
	return versions, higher, lower
}
