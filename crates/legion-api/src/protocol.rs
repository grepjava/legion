//! Which builds work together (PLAN.md, phase 6). A build speaks one
//! protocol, the meaning of what managers and agents say to each other and
//! of the commands in the managers' log, and works with peers back to an
//! oldest one. Two builds mix when each speaks at least the other's oldest:
//! an upgrade goes one protocol at a time, every node through it before the
//! next. A peer that says nothing is from before the handshake, and speaks
//! protocol 1.

use serde::{Deserialize, Serialize};

/// The protocol this build speaks.
pub const SPEAKS: u32 = 1;
/// The oldest protocol this build works with.
pub const OLDEST: u32 = 1;

/// What a build speaks, and the oldest it works with.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    #[serde(default)]
    pub speaks: u32,
    #[serde(default)]
    pub oldest: u32,
}

impl Protocol {
    /// This build's.
    pub const fn current() -> Protocol {
        Protocol {
            speaks: SPEAKS,
            oldest: OLDEST,
        }
    }

    /// As said by a peer: nothing said is protocol 1.
    pub fn said(speaks: u32, oldest: u32) -> Protocol {
        Protocol {
            speaks: speaks.max(1),
            oldest: oldest.max(1),
        }
    }

    pub fn is_unset(&self) -> bool {
        self.speaks == 0 && self.oldest == 0
    }

    /// Whether `self` and `peer` work together; if not, why, with `who`
    /// naming the peer.
    pub fn works_with(&self, peer: Protocol, who: &str) -> Result<(), String> {
        let (me, peer) = (
            Protocol::said(self.speaks, self.oldest),
            Protocol::said(peer.speaks, peer.oldest),
        );
        if peer.speaks < me.oldest {
            return Err(format!(
                "{who} speaks protocol {}, older than {}, the oldest this one works with: upgrade it first",
                peer.speaks, me.oldest
            ));
        }
        if me.speaks < peer.oldest {
            return Err(format!(
                "{who} works with protocol {} and newer, and this one speaks {}: upgrade this one first",
                peer.oldest, me.speaks
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(speaks: u32, oldest: u32) -> Protocol {
        Protocol { speaks, oldest }
    }

    #[test]
    fn builds_one_protocol_apart_mix() {
        assert!(p(2, 1).works_with(p(1, 1), "n1").is_ok());
        assert!(p(1, 1).works_with(p(2, 1), "n1").is_ok());
        // Nothing said is protocol 1.
        assert!(p(1, 1).works_with(p(0, 0), "n1").is_ok());
        assert!(p(2, 1).works_with(Protocol::default(), "n1").is_ok());
    }

    #[test]
    fn builds_further_apart_are_refused_saying_which_goes_first() {
        let old = p(3, 2).works_with(p(1, 1), "node n1").unwrap_err();
        assert_eq!(
            old,
            "node n1 speaks protocol 1, older than 2, the oldest this one works with: upgrade it first"
        );
        let new = p(1, 1).works_with(p(3, 2), "node n1").unwrap_err();
        assert_eq!(
            new,
            "node n1 works with protocol 2 and newer, and this one speaks 1: upgrade this one first"
        );
        assert!(p(3, 2).works_with(Protocol::default(), "n1").is_err());
    }
}
