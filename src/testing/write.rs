//! The Storage Write RPCs: `GetWriteStream`, `CreateWriteStream`, `AppendRows`, `FlushRows`,
//! `FinalizeWriteStream` and `BatchCommitWriteStreams`.

use crate::db::fake::FakeCall;
use crate::testing::server::FakeShared;

impl FakeShared {
    pub(super) async fn serve_write(&self, call: FakeCall) {
        // TODO: open the default and created streams, append proto, Arrow and CDC rows with
        // offsets, make them visible as each stream type does, apply reject_rows and faults.
        let described = call.method().to_string();
        self.unmatched(call, &described, &[]);
    }
}
