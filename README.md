# Autopilot

This library contains extended funcionality to use with [Consul autopilot](https://godoc.org/github.com/hashicorp/consul/agent/consul/autopilot).

## Functionality

* Allows setting non voting servers. Those servers won't be included as voters. This allows you to add more servers without impacting the number os servers that are included in the voting decissions.
* A zone can be specified to each server and if enabled, only one server per zone will act as voter.
* The version of the voters can be upgraded automatically.

## Usage

To use it you only need to do the following:

```go
package main

import (
    "github.com/hashicorp/consul/agent/consul/autopilot"
    improvedAutopilot "github.com/jorgemarey/autopilot"
)

func example() *autopilot.Autopilot {
    var defaultDelegate autopilot.Delegate // construct the default autopilot.Delegate
    ...
    apDelegate := improvedAutopilot.New(nil, defaultDelegate) // you can provide a logger if needed
    return autopilot.NewAutopilot(nil, apDelegate, 10 * time.Second, 10 * time.Second)
}
```
