use crate::types::*;

use parse_config::Config as ParseConfig;
use parse_config::ConfigError;

use std::net::IpAddr;
use std::net::SocketAddr;

use serde::Deserialize;

#[derive(Debug)]
pub enum Error {
    InvalidQuorumSize(Gid),
    Config(parse_config::ConfigError),
}

impl From<ConfigError> for Error {
    fn from(e: ConfigError) -> Error {
        Error::Config(e)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct PeerConfig {
    pub pid: Pid,
    pub ip: IpAddr,
    pub port: u16,
}

impl PeerConfig {
    pub fn addr(&self) -> SocketAddr {
        (self.ip, self.port).into()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct GroupConfig {
    pub gid: Gid,
    /// kept in `pid` order, no duplicates
    pub peers: Vec<PeerConfig>,
    pub quorum: u8,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Config {
    /// kept in `gid` order, no duplicates
    pub groups: Vec<GroupConfig>,
    pub leader_timeout_secs: usize,
    pub reconnect_timeout_secs: usize,
}

impl GroupConfig {
    pub fn peer(&self, pid: Pid) -> Option<&PeerConfig> {
        self.peers.iter().find(|it| it.pid == pid)
    }
}

impl Config {
    pub fn load(conf_file: &str) -> Result<Self, Error> {
        let settings = ParseConfig::builder()
            .set_default("leader_timeout_secs", 2 as u16)?
            .set_default("reconnect_timeout_secs", 2 as u16)?
            .add_source(parse_config::File::new(conf_file, parse_config::FileFormat::Yaml))
            .build()?;

        let config: Config = settings.try_deserialize()?;

        for g in &config.groups {
            if g.quorum as usize <= g.peers.len() / 2 || g.quorum as usize > g.peers.len() {
                return Err(Error::InvalidQuorumSize(g.gid));
            }
        }

        Ok(config)
    }

    pub fn new_for_test() -> Self {
        let mut groups = vec![];
        let mut ip = 1;
        for g in 0..2 {
            // 2 groups
            let mut peers = vec![];
            for p in 0..3 {
                // 3 peers per group
                peers.push(PeerConfig {
                    pid: Pid(p),
                    ip: format!("127.0.0.{}", ip).parse().unwrap(),
                    port: 10000,
                });
                ip += 1;
            }
            groups.push(GroupConfig {
                gid: Gid(g),
                peers,
                quorum: 2,
            });
        }

        Config {
            groups,
            leader_timeout_secs: 2,
            reconnect_timeout_secs: 2,
        }
    }

    pub fn group(&self, gid: Gid) -> Option<&GroupConfig> {
        self.groups.iter().find(|it| it.gid == gid)
    }

    pub fn group_pids(&self, gid: Gid) -> Option<Vec<Pid>> {
        self.group(gid).map(|g| g.peers.iter().map(|p| p.pid).collect())
    }

    pub fn quorum_size(&self, gid: Gid) -> Option<usize> {
        self.group(gid).map(|g| g.quorum as usize)
    }

    pub fn peer(&self, gid: Gid, pid: Pid) -> Option<&PeerConfig> {
        self.group(gid).and_then(|g| g.peers.iter().find(|it| it.pid == pid))
    }

    pub fn peers(&self, gid: Gid) -> Option<&Vec<PeerConfig>> {
        self.group(gid).map(|g| &g.peers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_test() {
        let config = Config::load("../example.yaml").unwrap();

        assert!(config.group(Gid(0)).is_some());
        assert!(config.group(Gid(1)).is_some());
        assert!(config.group(Gid(2)).is_none());

        assert!(config.peer(Gid(0), Pid(0)).is_some());
        assert!(config.peer(Gid(0), Pid(2)).is_some());
        assert!(config.peer(Gid(0), Pid(3)).is_none());

        assert!(config.peer(Gid(1), Pid(0)).is_some());
        assert!(config.peer(Gid(1), Pid(2)).is_some());
        assert!(config.peer(Gid(1), Pid(3)).is_none());

        assert_eq!(config, Config::new_for_test());
    }
}
