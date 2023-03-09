use std::{sync::Arc, backtrace::Backtrace, io::{self, Write}};

use parking_lot::MutexGuard;

use super::world::{Node, NodeId};

pub type Mutex<T> = parking_lot::Mutex<T>;

/// More deterministic condvar. Determenism comes from the fact that
/// at all times there is at most one running thread.
pub struct Condvar {
    waiters: Mutex<CondvarState>,
}

struct CondvarState {
    waiters: Vec<Arc<Park>>,
}

impl Condvar {
    pub fn new() -> Condvar {
        Condvar {
            waiters: Mutex::new(CondvarState { waiters: Vec::new() }),
        }
    }

    /// Blocks the current thread until this condition variable receives a notification.
    pub fn wait<'a, T>(&self, guard: &mut parking_lot::MutexGuard<'a, T>) {
        let park = Park::new(false);

        // add the waiter to the list
        self.waiters.lock().waiters.push(park.clone());

        parking_lot::MutexGuard::unlocked(guard, || {
            // part the thread, it will be woken up by notify_one or notify_all
            park.park();
        });
    }

    /// Wakes up all blocked threads on this condvar, can be called only from the node thread.
    pub fn notify_all(&self) {
        // TODO: check that it's waked up in random order and yield to the scheduler

        let mut waiters = self.waiters.lock().waiters.drain(..).collect::<Vec<_>>();
        for waiter in waiters.drain(..) {
            // block (park) the current thread, wake the other thread
            waiter.wake();
        }
    }

    /// Wakes up one blocked thread on this condvar, can be called only from the node thread.
    pub fn notify_one(&self) {
        // TODO: wake up random thread

        let to_wake = self.waiters.lock().waiters.pop();

        if let Some(waiter) = to_wake {
            // block (park) the current thread, wake the other thread
            waiter.wake();
        } else {
            // block (park) the current thread just in case
            Park::yield_thread()
        }
    }
}

/// A tool to block (park) a current thread until it will be woken up.
pub struct Park {
    lock: Mutex<ParkState>,
    cvar: parking_lot::Condvar,
}

struct ParkState {
    /// False means that thread cannot continue without external signal,
    /// i.e. waiting for some event to happen.
    can_continue: bool,
    /// False means that thread is unconditionally parked and waiting for
    /// world simulation to wake it up. True means that the parking is
    /// finished and the thread can continue.
    finished: bool,
    node_id: Option<NodeId>,
    backtrace: Option<Backtrace>,
}

impl Park {
    pub fn new(can_continue: bool) -> Arc<Park> {
        Arc::new(
            Park {
                lock: Mutex::new(ParkState {
                    can_continue,
                    finished: false,
                    node_id: None,
                    backtrace: None,
                }),
                cvar: parking_lot::Condvar::new(),
            }
        )
    }

    fn init_state(state: &mut ParkState, node: &Arc<Node>) {
        state.node_id = Some(node.id);
        state.backtrace = Some(Backtrace::capture());
    }

    /// Should be called once by the waiting thread. Blocks the thread until wake() is called,
    /// and until the thread is woken up by the world simulation.
    pub fn park(self: &Arc<Self>) {
        let node = Node::current();

        // start blocking
        let mut state = self.lock.lock();
        Self::init_state(&mut state, &node);

        if state.can_continue {
            // unconditional parking
            println!("YIELD PARKING: node {:?}", node.id);

            parking_lot::MutexGuard::unlocked(&mut state, || {
                // first put to world parking, then decrease the running threads counter
                node.internal_parking_middle(self.clone());
            });
        } else {
            println!("AWAIT PARKING: node {:?}", node.id);

            parking_lot::MutexGuard::unlocked(&mut state, || {
                // conditional parking, decrease the running threads counter without parking
                node.internal_parking_start();
            });

            // wait for condition
            while !state.can_continue {
                self.cvar.wait(&mut state);
            }

            println!("CONDITION MET: node {:?}", node.id);
            // condition is met, we are now running instead of the waker thread.
            // the next thing is to park the thread in the world, then decrease
            // the running threads counter
            node.internal_parking_middle(self.clone());
        }

        self.park_wait_the_world(node, &mut state);
    }

    fn park_wait_the_world(&self, node: Arc<Node>, state: &mut parking_lot::MutexGuard<ParkState>) {
        // condition is met, wait for world simulation to wake us up
        while !state.finished {
            self.cvar.wait(state);
        }

        println!("PARKING ENDED: node {:?}", node.id);

        // We are the only running thread now, we just need to update the state,
        // and continue the execution.
        node.internal_parking_end();
    }

    /// Hacky way to register parking before the thread is actually blocked.
    fn park_ahead_now() -> Arc<Park> {
        let park = Park::new(true);
        let node = Node::current();
        Self::init_state(&mut park.lock.lock(), &node);
        println!("PARKING MIDDLE alt: node {:?}", node.id);
        node.internal_parking_ahead(park.clone());
        park
    }

    /// Will wake up the thread that is currently conditionally parked. Can be called only
    /// from the node thread, because it will block the caller thread. What it will do:
    /// 1. Park the thread that called wake() in the world
    /// 2. Wake up the waiting thread (it will also park in the world)
    /// 3. Block the thread that called wake()
    pub fn wake(&self) {
        // parking the thread that called wake()
        let self_park = Park::park_ahead_now();

        let mut state = self.lock.lock();
        if state.can_continue {
            println!("WARN wake() called on a thread that is already waked, node {:?}", state.node_id);
            return;
        }
        state.can_continue = true;
        // and here we park the waiting thread
        self.cvar.notify_all();
        drop(state);
        
        // and here we block the thread that called wake() by defer
        let node = Node::current();
        let mut state = self_park.lock.lock();
        self_park.park_wait_the_world(node, &mut state);
    }

    /// Will wake up the thread that is currently unconditionally parked.
    pub fn internal_world_wake(&self) {
        let mut state = self.lock.lock();
        if state.finished {
            println!("WARN internal_world_wake() called on a thread that is already waked, node {:?}", state.node_id);
            return;
        }
        state.finished = true;
        self.cvar.notify_all();
    }

    /// Print debug info about the parked thread.
    pub fn debug_print(&self) {
        let state = self.lock.lock();
        println!("PARK: node {:?} wake1={} wake2={}", state.node_id, state.can_continue, state.finished);
        // println!("DEBUG: node {:?} wake1={} wake2={}, trace={:?}", state.node_id, state.can_continue, state.finished, state.backtrace);
    }

    /// It feels that this function can cause deadlocks.
    pub fn node_id(&self) -> Option<NodeId> {
        let state = self.lock.lock();
        state.node_id
    }

    /// Yield the current thread to the world simulation.
    pub fn yield_thread() {
        let park = Park::new(true);
        park.park();
    }
}
