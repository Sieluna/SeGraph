use std::cell::RefCell;
use std::collections::VecDeque;

/// Per-thread scratch buffers that are reused across queries without locking.
pub struct QueryContext {
    pub seen_set: Vec<u32>,
    pub seen_gen: u32,
    pub bfs_visited: Vec<u32>,
    pub bfs_gen: u32,
    pub queue_buf: VecDeque<(u32, u32)>,
}

impl QueryContext {
    #[must_use]
    pub fn new() -> Self {
        Self {
            seen_set: Vec::new(),
            seen_gen: 0,
            bfs_visited: Vec::new(),
            bfs_gen: 0,
            queue_buf: VecDeque::new(),
        }
    }

    /// Run a closure with access to the thread-local `QueryContext`.
    pub fn with<F, R>(f: F) -> R
    where
        F: FnOnce(&mut QueryContext) -> R,
    {
        CTX.with(|ctx| f(&mut ctx.borrow_mut()))
    }
}

impl Default for QueryContext {
    fn default() -> Self {
        Self::new()
    }
}

thread_local! {
    static CTX: RefCell<QueryContext> = RefCell::new(QueryContext::new());
}
