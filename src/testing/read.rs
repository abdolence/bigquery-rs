//! The Storage Read RPCs: `CreateReadSession` and `ReadRows`.

use crate::db::fake::FakeCall;
use crate::testing::server::FakeShared;

impl FakeShared {
    pub(super) async fn serve_read(&self, call: FakeCall) {
        // TODO: open sessions on a table's rows projected to the selected fields, spread over
        // its read streams, answer restricted sessions from the read rules, and apply faults.
        let described = call.method().to_string();
        let rules = self.rules().describe_reads();
        self.unmatched(call, &described, &rules);
    }
}
