package es

type RespSearchItem[T any] struct {
	Id     string `json:"_id"`
	Source T      `json:"_source"`
}

type RespBasic struct {
	Took     uint64 `json:"took"`
	TimedOut bool   `json:"timed_out"`
}

type RespSearch struct {
	RespBasic
	Hits struct {
		Total struct {
			Value    uint64 `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits interface{} `json:"hits"`
	} `json:"hits"`
}

type RespError struct {
	Status uint64 `json:"status"`
	Error  struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error"`
}
