package responsetypes

type Error struct {
	Message string `json:"error,omitempty"`
	Node    string `json:"node,omitempty"`
}

type OK struct {
	Result string `json:"result,omitempty"`
}

type GetRing struct {
	Nodes []string `json:"nodes"`
}

type GetDBs struct {
	Databases []string `json:"databases"`
}

type NodeStatus struct {
	Recovering   bool `json:"recovering"`
	Initializing bool `json:"initializing"`
}
