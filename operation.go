package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type Operation struct {
	val    *Client
	ApiLog func(op, idx, body string) func()
}

func NewOperation(c *Client) *Operation {
	return &Operation{val: c, ApiLog: c.ApiLog}
}

func (c *Operation) Search(idx string, body string, receive interface{}, ctx context.Context) (cnt uint64, err error) {
	reqData := &bytes.Buffer{}
	reqData.WriteString(body)

	defer c.ApiLog("_search", idx, body)()

	resp, err := c.val.Client.Search(c.val.Client.Search.WithIndex(idx), c.val.Client.Search.WithBody(reqData), c.val.Client.Search.WithContext(ctx))

	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			data := RespSearch{}
			data.Hits.Hits = receive
			if err = json.NewDecoder(resp.Body).Decode(&data); err == nil {
				cnt = data.Hits.Total.Value
			}
		} else {
			raw := RespError{}
			if err = json.NewDecoder(resp.Body).Decode(&raw); err == nil {
				err = fmt.Errorf(raw.Error.Reason)
			}
		}
	}

	return
}
