package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	App      AppConfig      `mapstructure:"app"`
	Postgres PostgresConfig `mapstructure:"postgres"`
	MongoDB  MongoConfig    `mapstructure:"mongodb"`
	Redis    RedisConfig    `mapstructure:"redis"`
}

type AppConfig struct {
	Env            string `mapstructure:"env"`
	Port           int    `mapstructure:"port"`
	JWTSecret      string `mapstructure:"jwt_secret"`
	JWTExpiryHours int    `mapstructure:"jwt_expiry_hours"`
}

type PostgresConfig struct {
	Shards             []ShardConfig `mapstructure:"shards"`
	MaxOpenConns       int           `mapstructure:"max_open_conns"`
	MaxIdleConns       int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetimeMin int           `mapstructure:"conn_max_lifetime_minutes"`
}

type ShardConfig struct {
	ID       int      `mapstructure:"id"`
	Primary  string   `mapstructure:"primary"`
	Replicas []string `mapstructure:"replicas"`
}

type MongoConfig struct {
	URI            string `mapstructure:"uri"`
	Database       string `mapstructure:"database"`
	ReadPreference string `mapstructure:"read_preference"`
}

type RedisConfig struct {
	Master    RedisNodeConfig   `mapstructure:"master"`
	Replicas  []RedisNodeConfig `mapstructure:"replicas"`
	Sentinel  SentinelConfig    `mapstructure:"sentinel"`
	TTL       RedisTTLConfig    `mapstructure:"ttl"`
	RateLimit RateLimitConfig   `mapstructure:"rate_limit"`
}

type RedisNodeConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

type SentinelConfig struct {
	MasterName string   `mapstructure:"master_name"`
	Addrs      []string `mapstructure:"addrs"`
	Password   string   `mapstructure:"password"`
}

type RedisTTLConfig struct {
	ProductSeconds     int `mapstructure:"product_seconds"`
	UserSessionSeconds int `mapstructure:"user_session_seconds"`
	CartSeconds        int `mapstructure:"cart_seconds"`
	RateLimitSeconds   int `mapstructure:"rate_limit_seconds"`
}

type RateLimitConfig struct {
	RequestsPerMinute int `mapstructure:"requests_per_minute"`
}

func (c *RedisTTLConfig) ProductTTL() time.Duration {
	return time.Duration(c.ProductSeconds) * time.Second
}

func (c *RedisTTLConfig) UserSessionTTL() time.Duration {
	return time.Duration(c.UserSessionSeconds) * time.Second
}

func (c *RedisTTLConfig) CartTTL() time.Duration {
	return time.Duration(c.CartSeconds) * time.Second
}

func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshalling config: %w", err)
	}
	return &cfg, nil
}
