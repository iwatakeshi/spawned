use crate::link::Exit;

/// Internal mailbox item used uniformly in both `tasks` and `threads` actor loops.
pub(crate) enum MailboxItem<M> {
    Message(M),
    Exit(Exit),
    Shutdown,
}
