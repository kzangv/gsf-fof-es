package es

import (
	"fmt"
	"gitee.com/kzangv/gsf-fof"
	"gitee.com/kzangv/gsf-fof/logger"
	"github.com/urfave/cli/v2"
	"strings"
)

type Config struct {
	MaxConnTime   int `json:"max_con_time"    yaml:"max_con_time"`
	MaxIdleTime   int `json:"max_idle_time"   yaml:"max_idle_time"`
	KeepAliveTime int `json:"keep_alive_time" yaml:"keep_alive_time"`
	SlowThreshold int `json:"slow_threshold"  yaml:"slow_threshold"`
}

type Component struct {
	DSNPrint bool
	Cfg      Config
	Clts     map[string]*Client
}

func (c *Component) CliFlags() []cli.Flag {
	fs := make([][]cli.Flag, 0, len(c.Clts)+1)
	fs = append(fs, []cli.Flag{
		&cli.IntFlag{Name: "es-conn-timeout", Usage: "es connect timeout", Action: func(_ *cli.Context, i int) error { c.Cfg.MaxConnTime = i; return nil }},
		&cli.IntFlag{Name: "es-idle-timeout", Usage: "es idle connect timeout", Action: func(_ *cli.Context, i int) error { c.Cfg.MaxIdleTime = i; return nil }},
		&cli.IntFlag{Name: "es-keepalive-time", Usage: "es keepalive time", Action: func(_ *cli.Context, i int) error { c.Cfg.KeepAliveTime = i; return nil }},
		&cli.IntFlag{Name: "es-slow-threshold", Usage: "es slow threshold", Action: func(_ *cli.Context, i int) error { c.Cfg.SlowThreshold = i; return nil }},
	})
	for name, clt := range c.Clts {
		clt.ref = c
		fs = append(fs, clt.CliFlags(name))
	}
	l := 0
	for k := range fs {
		l += len(fs[k])
	}
	ret := make([]cli.Flag, 0, l)
	for k := range fs {
		ret = append(ret, fs[k]...)
	}
	return ret
}

func (c *Component) Init(log logger.Interface, cfg gsf.Config) error {
	// 打印链接信息
	for name, clt := range c.Clts {
		msg := fmt.Sprintf("Init ElasticSearch (%s)", name)
		if cfg.LogMore() && c.DSNPrint {
			msg = fmt.Sprintf("%s [%s]", msg, strings.Join(clt.Cfg.Host, ";"))
		}
		log.Warn(msg)
	}
	return nil
}

func (c *Component) Run(log logger.Interface, appCfg gsf.Config) error {
	for name, clt := range c.Clts {
		if err := clt.Load(c, name, &c.Cfg, appCfg, log); err != nil {
			return err
		}
	}
	return nil
}

func (c *Component) Close(_ logger.Interface, _ gsf.Config) (err error) {
	return nil
}
