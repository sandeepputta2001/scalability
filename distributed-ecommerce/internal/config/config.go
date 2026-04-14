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
	Kafka    KafkaConfig    `mapstructure:"kafka"`
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
	// Standalone mode (single master + replicas + sentinel)
	// Used when cluster.enabled = false
	Master   RedisNodeConfig   `mapstructure:"master"`
	Replicas []RedisNodeConfig `mapstructure:"replicas"`
	Sentinel SentinelConfig    `mapstructure:"sentinel"`

	// Cluster mode (sharded — 3 masters × 1 replica each)
	// Used when cluster.enabled = true
	Cluster RedisClusterConfig `mapstructure:"cluster"`

	TTL       RedisTTLConfig  `mapstructure:"ttl"`
	RateLimit RateLimitConfig `mapstructure:"rate_limit"`
}

// RedisClusterConfig configures Redis Cluster (sharding + replication built-in).
// Redis Cluster shards data across N masters using CRC16 hash slots (0-16383).
// Each master owns a contiguous range of slots; replicas mirror their master.
type RedisClusterConfig struct {
	Enabled  bool     `mapstructure:"enabled"`
	Addrs    []string `mapstructure:"addrs"` // all nodes (masters + replicas)
	Password string   `mapstructure:"password"`
	// ReadFromReplica: route read commands to replica nodes.
	// Tradeoff: lower master load + higher read throughput vs. stale reads.
	ReadFromReplica bool `mapstructure:"read_from_replica"`
	// MaxRedirects: how many MOVED/ASK redirects to follow before giving up.
	MaxRedirects int `mapstructure:"max_redirects"`
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

// ─── Kafka ────────────────────────────────────────────────────────────────────

type KafkaConfig struct {
	Brokers           []string `mapstructure:"brokers"`
	ReplicationFactor int      `mapstructure:"replication_factor"`
	AutoCreateTopics  bool     `mapstructure:"auto_create_topics"`
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
