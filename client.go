package es

import (
	"crypto/tls"
	"fmt"
	"gitee.com/kzangv/gsf-fof"
	"gitee.com/kzangv/gsf-fof/logger"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/urfave/cli/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type ClientConfig struct {
	Host        []string `json:"host"       yaml:"host"`
	Pwd         string   `json:"password"   yaml:"password"`
	User        string   `json:"user"       yaml:"user"`
	MaxIdleConn int      `json:"idle_limit" yaml:"idle_limit"`
}

type Client struct {
	Client       *elasticsearch.Client
	Cfg          ClientConfig
	ref          *Component
	ApiLog       func(op, idx, body string) func()
	CreateClient func(c *ClientConfig, cc *Config) (es *elasticsearch.Client, err error)
}

func (c *Client) Ref() *Component {
	return c.ref
}

func (c *Client) CliFlags(name string) []cli.Flag {
	return []cli.Flag{
		&cli.IntFlag{
			Name:   fmt.Sprintf("es-%s-idle-limit", name),
			Usage:  fmt.Sprintf("es(%s) idle max count", name),
			Action: func(_ *cli.Context, v int) error { c.Cfg.MaxIdleConn = v; return nil },
		},
		&cli.StringFlag{
			Name:  fmt.Sprintf("es-%s-dsn", name),
			Usage: fmt.Sprintf("es(%s) DSN (format: `{{user}}:{{password}}@{{host}},{{host}}`)", name),
			Action: func(_ *cli.Context, val string) error {
				dsn := strings.Split(val, "@")
				switch len(dsn) {
				case 1:
					c.Cfg.Host = strings.Split(dsn[0], ",")
				case 2:
					auth := strings.Split(dsn[0], ":")
					if len(auth) != 2 {
						return fmt.Errorf("ElasticSearch DSN is invalid(%s)", val)
					}
					var (
						u, p string
						err  error
					)
					if u, err = url.QueryUnescape(auth[0]); err == nil {
						if p, err = url.QueryUnescape(auth[1]); err == nil {
							c.Cfg.Host = strings.Split(dsn[1], ",")
							c.Cfg.User, c.Cfg.Pwd = u, p
						}
					}
					if err != nil {
						return fmt.Errorf("ElasticSearch decode user is faild(%s), err-msg: %s", val, err.Error())
					}
				default:
					return fmt.Errorf("ElasticSearch DSN is invalid(%s)", val)
				}
				return nil
			}},
	}
}

func (c *Client) Load(r *Component, name string, cfg *Config, appCfg gsf.Config, log logger.Interface) (err error) {
	if c.CreateClient == nil {
		c.CreateClient = DefaultNewClient
	}
	c.Client, err = c.CreateClient(&c.Cfg, cfg)
	if err != nil {
		err = fmt.Errorf("Load ElasticSearch (%s) Failed (Error: %s) ", name, err.Error())
		c.Client = nil
	}

	c.ApiLog = func(op, idx, body string) func() {
		bTm := time.Now()
		return func() {
			spendTime := time.Now().Sub(bTm) / time.Millisecond
			if spendTime > time.Duration(c.ref.Cfg.SlowThreshold) { // 查询超过阈值
				log.Warn(" ES[%d ms]: %s/%s -- %s", spendTime, idx, op, body)
			} else if appCfg.Env() == gsf.EnvLocal {
				log.Info(" ES[%d ms]: %s/%s -- %s", spendTime, idx, op, body)
			}
		}
	}
	return err
}

func DefaultNewClient(c *ClientConfig, cc *Config) (es *elasticsearch.Client, err error) {
	return elasticsearch.NewClient(elasticsearch.Config{
		Addresses: c.Host,
		Username:  c.User,
		Password:  c.Pwd,
		Transport: &http.Transport{
			TLSClientConfig:    &tls.Config{InsecureSkipVerify: true},
			DisableCompression: true,
			DialContext: (&net.Dialer{
				Timeout:   time.Duration(cc.MaxConnTime) * time.Second,
				KeepAlive: time.Duration(cc.KeepAliveTime) * time.Second,
			}).DialContext,
			MaxIdleConns:          c.MaxIdleConn,
			MaxIdleConnsPerHost:   c.MaxIdleConn,
			IdleConnTimeout:       time.Duration(cc.MaxIdleTime) * time.Second,
			TLSHandshakeTimeout:   time.Duration(cc.MaxConnTime) * time.Second / 2,
			ExpectContinueTimeout: time.Second,
		},
	})
}
