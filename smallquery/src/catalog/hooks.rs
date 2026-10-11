//! Points in the commit protocol where a test holds one commit while others proceed, so that the
//! interleavings the protocol must survive are reproduced exactly rather than raced for. Outside
//! tests a point is a no-op.

/// A point one commit attempt, or one load, passes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CommitPoint {
    /// A load has read the newest pointer, a live one, and not yet the metadata it names.
    PointerRead,
    /// The attempt has loaded the pointer it builds on and has written nothing.
    Loaded,
    /// The attempt's pointer create succeeded and its predecessor is not read back yet.
    Created,
}

#[derive(Debug, Default)]
pub(crate) struct CommitHooks {
    #[cfg(test)]
    holds: std::sync::Mutex<std::collections::HashMap<CommitPoint, testing::Hold>>,
    #[cfg(test)]
    panics: std::sync::Mutex<std::collections::HashSet<CommitPoint>>,
}

impl CommitHooks {
    /// Called by every attempt at `point`; returns once a test holding it releases it.
    pub(crate) async fn reach(&self, point: CommitPoint) {
        #[cfg(test)]
        {
            let panics = self
                .panics
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&point);
            assert!(!panics, "a test panics the attempt at {point:?}");
            let hold = self
                .holds
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&point);
            if let Some(hold) = hold {
                hold.wait().await;
            }
        }
        #[cfg(not(test))]
        let _ = point;
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::{CommitHooks, CommitPoint};
    use tokio::sync::oneshot;

    /// The holding side of a point: the first attempt that reaches it waits here.
    #[derive(Debug)]
    pub(crate) struct Hold {
        reached: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    }

    impl Hold {
        pub(super) async fn wait(self) {
            let _ = self.reached.send(());
            let _ = self.release.await;
        }
    }

    /// The test's side of a held point.
    #[derive(Debug)]
    pub(crate) struct Held {
        reached: oneshot::Receiver<()>,
        release: oneshot::Sender<()>,
    }

    impl Held {
        /// Waits until an attempt reaches the point and stops there.
        pub(crate) async fn reached(&mut self) {
            let _ = (&mut self.reached).await;
        }

        /// Lets the held attempt go on.
        pub(crate) fn release(self) {
            let _ = self.release.send(());
        }
    }

    impl CommitHooks {
        /// Holds the next attempt that reaches `point`; later attempts pass it.
        pub(crate) fn hold(&self, point: CommitPoint) -> Held {
            let (reached_tx, reached_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel();
            self.holds
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(
                    point,
                    Hold {
                        reached: reached_tx,
                        release: release_rx,
                    },
                );
            Held {
                reached: reached_rx,
                release: release_tx,
            }
        }

        /// Makes the next attempt that reaches `point` panic, as a defect past it would.
        pub(crate) fn panic_at(&self, point: CommitPoint) {
            self.panics
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(point);
        }
    }
}
