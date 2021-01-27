package autopilot

import "github.com/hashicorp/go-hclog"

// Option is an option to be used when creating a new Autopilot instance
type Option func(*ImprovedPromoter)

// WithLogger returns an Option to set the Autopilot instance's logger
func WithLogger(logger hclog.Logger) Option {
	if logger == nil {
		logger = hclog.Default()
	}

	return func(p *ImprovedPromoter) {
		p.logger = logger.Named("promoter")
	}
}
