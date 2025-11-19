/*
 *  Copyright (C) 2011-2017 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * This class is copied from the Tupl project and modified slightly. In particular, it doesn't
 * support LatchCondition, and it doesn't use Parker. Also, modified to implement the Lock
 * interface for acquiring shared locks.
 */

package filodb.memory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.util.concurrent.TimeUnit;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

/**
 * Non-reentrant read-write latch, designed for throughput over fairness. Implementation
 * doesn't track thread ownership or check for illegal usage. As a result, it typically
 * outperforms ReentrantLock and built-in Java synchronization. Although latch acquisition is
 * typically unfair, waiting threads aren't starved indefinitely.
 *
 * @author Brian S O'Neill
 */
public class Latch implements Lock {
    public static final int UNLATCHED = 0, EXCLUSIVE = 0x80000000, SHARED = 1;

    static final int SPIN_LIMIT = Runtime.getRuntime().availableProcessors() > 1 ? 1 << 10 : 1;

    static final VarHandle cStateHandle, cFirstHandle, cLastHandle,
        cWaiterHandle, cWaitStateHandle, cPrevHandle, cNextHandle;

    static {
        try {
            cStateHandle =
                MethodHandles.lookup().findVarHandle
                (Latch.class, "mLatchState", int.class);

            cFirstHandle =
                MethodHandles.lookup().findVarHandle
                (Latch.class, "mLatchFirst", WaitNode.class);

            cLastHandle =
                MethodHandles.lookup().findVarHandle
                (Latch.class, "mLatchLast", WaitNode.class);

            cWaiterHandle =
                MethodHandles.lookup().findVarHandle
                (WaitNode.class, "mWaiter", Object.class);

            cWaitStateHandle =
                MethodHandles.lookup().findVarHandle
                (WaitNode.class, "mWaitState", int.class);

            cPrevHandle =
                MethodHandles.lookup().findVarHandle
                (WaitNode.class, "mPrev", WaitNode.class);

            cNextHandle =
                MethodHandles.lookup().findVarHandle
                (WaitNode.class, "mNext", WaitNode.class);
        } catch (Throwable e) {
            throw new Error(e);
        }
    }

    private static void uncaught(Throwable e) {
        Thread t = Thread.currentThread();
        t.getUncaughtExceptionHandler().uncaughtException(t, e);
    }

    /*
      unlatched:           0               latch is available
      shared:              1..0x7fffffff   latch is held shared
      exclusive:  0x80000000               latch is held exclusively
      illegal:    0x80000001..0xffffffff   illegal exclusive state
     */ 
    volatile int mLatchState;

    // Queue of waiting threads.
    private volatile WaitNode mLatchFirst;
    private volatile WaitNode mLatchLast;

    public Latch() {
    }

    /**
     * @param initialState UNLATCHED, EXCLUSIVE, or SHARED
     */
    public Latch(int initialState) {
        // Assume that this latch instance is published to other threads safely, and so a
        // volatile store isn't required.
        cStateHandle.set(this, initialState);
    }

    @Override
    public void lock() {
        acquireShared();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        acquireSharedInterruptibly();
    }

    @Override
    public boolean tryLock() {
        return tryAcquireShared();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return tryAcquireSharedNanos(unit.toNanos(time));
    }

    @Override
    public void unlock() {
        releaseShared();
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    boolean isHeldExclusive() {
        return mLatchState == EXCLUSIVE;
    }

    /**
     * Try to acquire the exclusive latch, barging ahead of any waiting threads if possible.
     */
    public boolean tryAcquireExclusive() {
        return doTryAcquireExclusive();
    }

    private boolean doTryAcquireExclusive() {
        return mLatchState == 0 && cStateHandle.compareAndSet(this, 0, EXCLUSIVE);
    }

    private void doAcquireExclusiveSpin() {
        while (!doTryAcquireExclusive()) {
            Thread.onSpinWait();
        }
    }

    /**
     * Attempt to acquire the exclusive latch, aborting if interrupted.
     *
     * @param nanosTimeout pass negative for infinite timeout
     */
    public boolean tryAcquireExclusiveNanos(long nanosTimeout) throws InterruptedException {
        return doTryAcquireExclusiveNanos(nanosTimeout);
    }

    private boolean doTryAcquireExclusiveNanos(long nanosTimeout) throws InterruptedException {
        if (doTryAcquireExclusive()) {
            return true;
        }

        if (nanosTimeout == 0) {
            return false;
        }

        boolean result;
        try {
            result = acquire(new Timed(nanosTimeout));
        } catch (Throwable e) {
            // Possibly an OutOfMemoryError.
            if (nanosTimeout < 0) {
                // Caller isn't expecting an exception, so spin.
                doAcquireExclusiveSpin();
                return true;
            }
            return false;
        }

        return checkTimedResult(result, nanosTimeout);
    }

    /**
     * Acquire the exclusive latch, barging ahead of any waiting threads if possible.
     */
    public void acquireExclusive() {
        if (!doTryAcquireExclusive()) {
            doAcquireExclusive();
        }
    }

    /**
     * Caller should have already called tryAcquireExclusive.
     */
    private void doAcquireExclusive() {
        try {
            acquire(new WaitNode());
        } catch (Throwable e) {
            // Possibly an OutOfMemoryError. Caller isn't expecting an exception, so spin.
            doAcquireExclusiveSpin();
        }
    }

    /**
     * Acquire the exclusive latch, aborting if interrupted.
     */
    public void acquireExclusiveInterruptibly() throws InterruptedException {
        doTryAcquireExclusiveNanos(-1);
    }

    /**
     * Invokes the given continuation upon the latch being acquired exclusively. When acquired,
     * the continuation is run by the current thread, or it's enqueued to be run by a thread
     * which releases the latch. The releasing thread actually retains the latch and runs the
     * continuation, effectively transferring latch ownership. The continuation must not
     * explicitly release the latch, although it can downgrade the latch. Any exception thrown
     * by the continuation is passed to the uncaught exception handler of the running thread,
     * and then the latch is released.
     *
     * @param cont called with latch held
     */
    public void uponExclusive(Runnable cont) {
        if (!doTryAcquireExclusive()) enqueue: {
            WaitNode node;
            try {
                node = new WaitNode(cont, WaitNode.SIGNALED);
            } catch (Throwable e) {
                // Possibly an OutOfMemoryError. Caller isn't expecting an exception, so spin.
                doAcquireExclusiveSpin();
                break enqueue;
            }

            WaitNode prev = enqueue(node);

            boolean acquired = doTryAcquireExclusive();

            if (node.mWaiter == null) {
                // Continuation already ran or is running right now.
                if (acquired) {
                    releaseExclusive();
                }
                return;
            }

            if (!acquired) {
                return;
            }

            cWaiterHandle.setOpaque(node, null);

            // Acquired while still in the queue. Remove the node now, releasing memory.
            if (mLatchFirst != node) {
                remove(node, prev);
            } else {
                removeFirst(node);
            }
        }

        try {
            cont.run();
        } catch (Throwable e) {
            uncaught(e);
        }

        releaseExclusive();
    }

    /**
     * Downgrade the held exclusive latch into a shared latch. Caller must later call
     * releaseShared instead of releaseExclusive.
     */
    public final void downgrade() {
        mLatchState = 1;

        while (true) {
            // Sweep through the queue, waking up a contiguous run of shared waiters.
            final WaitNode first = first();
            if (first == null) {
                return;
            }

            WaitNode node = first;
            while (true) {
                Object waiter = node.mWaiter;
                if (waiter != null) {
                    if (node instanceof Shared) {
                        cStateHandle.getAndAdd(this, 1);
                        if (cWaiterHandle.compareAndSet(node, waiter, null)) {
                            LockSupport.unpark((Thread) waiter);
                        } else {
                            // Already unparked, so fix the share count.
                            cStateHandle.getAndAdd(this, -1);
                        }
                    } else {
                        if (node != first) {
                            // Advance the queue past any shared waiters that were encountered.
                            mLatchFirst = node;
                        }
                        return;
                    }
                }

                WaitNode next = node.mNext;

                if (next == null) {
                    // Queue is now empty, unless an enqueue is in progress.
                    if (cLastHandle.compareAndSet(this, node, null)) {
                        cFirstHandle.compareAndSet(this, first, null);
                        return;
                    }
                    // Sweep from the start again.
                    break;
                }

                node = next;
            }
        }
    }

    /**
     * Release the held exclusive latch.
     */
    public final void releaseExclusive() {
        int trials = 0;
        while (true) {
            WaitNode last = mLatchLast;

            if (last == null) {
                // No waiters, so release the latch.
                mLatchState = 0;

                // Need to check if any waiters again, due to race with enqueue. If cannot
                // immediately re-acquire the latch, then let the new owner (which barged in)
                // unpark the waiters when it releases the latch.
                last = mLatchLast;
                if (last == null || !cStateHandle.compareAndSet(this, 0, EXCLUSIVE)) {
                    return;
                }
            }

            // Although the last waiter has been observed to exist, the first waiter field
            // might not be set yet.
            WaitNode first = mLatchFirst;

            unpark: if (first != null) {
                Object waiter = first.mWaiter;

                if (waiter != null) {
                    if (first instanceof Shared) {
                        // TODO: can this be combined into one downgrade step?
                        downgrade();
                        if (doReleaseShared()) {
                            return;
                        }
                        trials = 0;
                        continue;
                    }

                    if (first.mWaitState != WaitNode.SIGNALED) {
                        // Unpark the waiter, but allow another thread to barge in.
                        mLatchState = 0;
                        LockSupport.unpark((Thread) waiter);
                        return;
                    }
                }

                // Remove first from the queue.
                {
                    WaitNode next = first.mNext;
                    if (next != null) {
                        mLatchFirst = next;
                    } else {
                        // Queue is now empty, unless an enqueue is in progress.
                        if (last != first || !cLastHandle.compareAndSet(this, last, null)) {
                            break unpark;
                        }
                        cFirstHandle.compareAndSet(this, last, null);
                    }
                }

                if (waiter != null && cWaiterHandle.compareAndSet(first, waiter, null)) {
                    // Fair handoff to waiting thread or continuation.
                    if (waiter instanceof Thread) {
                        LockSupport.unpark((Thread) waiter);
                        return;
                    }
                    try {
                        ((Runnable) waiter).run();
                    } catch (Throwable e) {
                        uncaught(e);
                    }
                    if (!isHeldExclusive()) {
                        if (mLatchState <= 0) {
                            throw new IllegalStateException
                                ("Illegal latch state: " + mLatchState + ", caused by " + waiter);
                        }
                        if (doReleaseShared()) {
                            return;
                        }
                    }
                    trials = 0;
                    continue;
                }
            }

            trials = spin(trials);
        }
    }

    /**
     * Convenience method, which releases the held exclusive or shared latch.
     *
     * @param exclusive call releaseExclusive if true, else call releaseShared
     */
    public final void release(boolean exclusive) {
        if (exclusive) {
            releaseExclusive();
        } else {
            releaseShared();
        }
    }

    /**
     * Releases an exclusive or shared latch.
     */
    public final void releaseEither() {
        if (((int) cStateHandle.get(this)) == EXCLUSIVE) {
            releaseExclusive();
        } else {
            releaseShared();
        }
    }

    /**
     * Try to acquire a shared latch, barging ahead of any waiting threads if possible.
     */
    public boolean tryAcquireShared() {
        return doTryAcquireShared();
    }

    private boolean doTryAcquireShared() {
        WaitNode first = mLatchFirst;
        if (first != null && !(first instanceof Shared)) {
            return false;
        }
        int state = mLatchState;
        return state >= 0 && cStateHandle.compareAndSet(this, state, state + 1);
    }

    private void doAcquireSharedSpin() {
        while (!doTryAcquireShared()) {
            Thread.onSpinWait();
        }
    }

    /**
     * Attempt to acquire a shared latch, aborting if interrupted.
     *
     * @param nanosTimeout pass negative for infinite timeout
     */
    public boolean tryAcquireSharedNanos(long nanosTimeout) throws InterruptedException {
        return doTryAcquireSharedNanos(nanosTimeout);
    }

    private final boolean doTryAcquireSharedNanos(long nanosTimeout) throws InterruptedException {
        WaitNode first = mLatchFirst;
        if (first == null || first instanceof Shared) {
            int trials = 0;
            int state;
            while ((state = mLatchState) >= 0) {
                if (cStateHandle.compareAndSet(this, state, state + 1)) {
                    return true;
                }
                // Spin even if timeout is zero. The timeout applies to a blocking acquire.
                trials = spin(trials);
            }
        }

        if (nanosTimeout == 0) {
            return false;
        }

        boolean result;
        try {
            result = acquire(new TimedShared(nanosTimeout));
        } catch (Throwable e) {
            // Possibly an OutOfMemoryError.
            if (nanosTimeout < 0) {
                // Caller isn't expecting an exception, so spin.
                doAcquireSharedSpin();
                return true;
            }
            return false;
        }

        return checkTimedResult(result, nanosTimeout);
    }

    private static boolean checkTimedResult(boolean result, long nanosTimeout)
        throws InterruptedException
    {
        if (!result && (Thread.interrupted() || nanosTimeout < 0)) {
            InterruptedException e;
            try {
                e = new InterruptedException();
            } catch (Throwable e2) {
                // Possibly an OutOfMemoryError.
                if (nanosTimeout < 0) {
                    throw e2;
                }
                return false;
            }
            throw e;
        }

        return result;
    }

    /**
     * Like tryAcquireShared, except blocks if an exclusive latch is held.
     *
     * @return false if not acquired due to contention with other shared requests
     */
    public boolean acquireSharedUncontended() {
        WaitNode first = mLatchFirst;
        if (first == null || first instanceof Shared) {
            int state = mLatchState;
            if (state >= 0) {
                return cStateHandle.compareAndSet(this, state, state + 1);
            }
        }

        try {
            acquire(new Shared());
        } catch (Throwable e) {
            // Possibly an OutOfMemoryError. Caller isn't expecting an exception, so spin.
            doAcquireSharedSpin();
        }

        return true;
    }

    /**
     * Like tryAcquireSharedNanos, except blocks if an exclusive latch is held.
     *
     * @param nanosTimeout pass negative for infinite timeout
     * @return -1 if not acquired due to contention with other shared requests, 0 if timed out,
     * or 1 if acquired
     */
    public int acquireSharedUncontendedNanos(long nanosTimeout) throws InterruptedException {
        WaitNode first = mLatchFirst;
        if (first == null || first instanceof Shared) {
            int state = mLatchState;
            if (state >= 0) {
                return cStateHandle.compareAndSet(this, state, state + 1) ? 1 : -1;
            }
        }

        boolean result;
        try {
            result = acquire(new TimedShared(nanosTimeout));
        } catch (Throwable e) {
            // Possibly an OutOfMemoryError.
            if (nanosTimeout < 0) {
                // Caller isn't expecting an exception, so spin.
                doAcquireSharedSpin();
                return 1;
            }
            return 0;
        }

        return checkTimedResult(result, nanosTimeout) ? 1 : 0;
    }

    /**
     * Acquire a shared latch, barging ahead of any waiting threads if possible.
     */
    public void acquireShared() {
        if (!tryAcquireSharedSpin()) {
            try {
                acquire(new Shared());
            } catch (Throwable e) {
                // Possibly an OutOfMemoryError. Caller isn't expecting an exception, so spin.
                doAcquireSharedSpin();
            }
        }
    }

    private boolean tryAcquireSharedSpin() {
        WaitNode first = mLatchFirst;
        if (first == null || first instanceof Shared) {
            int trials = 0;
            int state;
            while ((state = mLatchState) >= 0) {
                if (cStateHandle.compareAndSet(this, state, state + 1)) {
                    return true;
                }
                trials = spin(trials);
            }
        }
        return false;
    }

    /**
     * Acquire a shared latch, aborting if interrupted.
     */
    public void acquireSharedInterruptibly() throws InterruptedException {
        doTryAcquireSharedNanos(-1);
    }

    /**
     * Attempt to upgrade a held shared latch into an exclusive latch. Upgrade fails if shared
     * latch is held by more than one thread. If successful, caller must later call
     * releaseExclusive instead of releaseShared.
     */
    public boolean tryUpgrade() {
        return doTryUpgrade();
    }

    private boolean doTryUpgrade() {
        while (true) {
            if (mLatchState != 1) {
                return false;
            }
            if (cStateHandle.compareAndSet(this, 1, EXCLUSIVE)) {
                return true;
            }
            Thread.onSpinWait();
        }
    }

    /**
     * Release a held shared latch.
     */
    public void releaseShared() {
        int trials = 0;
        while (true) {
            int state = mLatchState;

            WaitNode last = mLatchLast;
            if (last == null) {
                // No waiters, so release the latch.
                if (cStateHandle.compareAndSet(this, state, --state)) {
                    if (state == 0) {
                        // Need to check if any waiters again, due to race with enqueue. If
                        // cannot immediately re-acquire the latch, then let the new owner
                        // (which barged in) unpark the waiters when it releases the latch.
                        last = mLatchLast;
                        if (last != null && cStateHandle.compareAndSet(this, 0, EXCLUSIVE)) {
                            releaseExclusive();
                        }
                    }
                    return;
                }
            } else if (state == 1) {
                // Try to switch to exclusive, and then let releaseExclusive deal with
                // unparking the waiters.
                if (cStateHandle.compareAndSet(this, 1, EXCLUSIVE) || doTryUpgrade()) {
                    releaseExclusive();
                    return;
                }
            } else if (cStateHandle.compareAndSet(this, state, --state)) {
                return;
            }

            trials = spin(trials);
        }
    }

    /**
     * @return false if latch is held exclusive now
     */
    private boolean doReleaseShared() {
        // Note: Same as regular releaseShared, except doesn't recurse into the
        // releaseExclusive method.

        int trials = 0;
        while (true) {
            int state = mLatchState;

            WaitNode last = mLatchLast;
            if (last == null) {
                if (cStateHandle.compareAndSet(this, state, --state)) {
                    if (state == 0) {
                        last = mLatchLast;
                        if (last != null && cStateHandle.compareAndSet(this, 0, EXCLUSIVE)) {
                            return false;
                        }
                    }
                    return true;
                }
            } else if (state == 1) {
                if (cStateHandle.compareAndSet(this, 1, EXCLUSIVE) || doTryUpgrade()) {
                    return false;
                }
            } else if (cStateHandle.compareAndSet(this, state, --state)) {
                return true;
            }

            trials = spin(trials);
        }
    }

    private boolean acquire(final WaitNode node) {
        node.mWaiter = Thread.currentThread();
        WaitNode prev = enqueue(node);
        int acquireResult = node.tryAcquire(this);

        if (acquireResult < 0) {
            int denied = 0;
            while (true) {
                boolean parkAbort = node.parkNow(this);

                acquireResult = node.tryAcquire(this);

                if (acquireResult >= 0) {
                    // Latch acquired after parking.
                    break;
                }

                if (parkAbort) {
                    if (!cWaiterHandle.compareAndSet(node, Thread.currentThread(), null)) {
                        // Fair handoff just occurred.
                        return true;
                    }

                    int state = mLatchState;
                    if (state >= 0) {
                        // Unpark any waiters that queued behind this request.
                        WaitNode wnode = node;
                        while ((wnode = wnode.mNext) != null) {
                            Object waiter = wnode.mWaiter;
                            if (waiter instanceof Thread) {
                                if (wnode instanceof Shared) {
                                    LockSupport.unpark((Thread) waiter);
                                } else {
                                    if (state == 0) {
                                        LockSupport.unpark((Thread) waiter);
                                    }
                                    // No need to iterate past an exclusive waiter.
                                    break;
                                }
                            }
                        }
                    }

                    // Remove the node from the queue. If it's the first, it cannot be safely
                    // removed without the latch having been properly acquired. So let it
                    // linger around until the latch is released.
                    if (prev != null) {
                        remove(node, prev);
                    }

                    return false;
                }

                // Lost the race. Request fair handoff.

                if (denied++ == 0) {
                    node.mWaitState = WaitNode.SIGNALED;
                }
            }
        }

        if (acquireResult == 0) {
            // Remove the node now, releasing memory.
            if (mLatchFirst != node) {
                remove(node, prev);
            } else {
                removeFirst(node);
            }
        }

        return true;
    }

    private void removeFirst(WaitNode node) {
        // Removing the first node requires special attention. Because the latch is now held by
        // the current thread, no other dequeues are in progress, but enqueues still are.

        while (true) {
            WaitNode next = node.mNext;
            if (next != null) {
                mLatchFirst = next;
                return;
            } else {
                // Queue is now empty, unless an enqueue is in progress.
                WaitNode last = mLatchLast;
                if (last == node && cLastHandle.compareAndSet(this, last, null)) {
                    cFirstHandle.compareAndSet(this, last, null);
                    return;
                }
            }
        }
    }

    private WaitNode enqueue(final WaitNode node) {
        var prev = (WaitNode) cLastHandle.getAndSet(this, node);

        if (prev == null) {
            mLatchFirst = node;
        } else {
            prev.mNext = node;
            WaitNode pp = prev.mPrev;
            if (pp != null) {
                // The old last node was intended to be removed, but the last node cannot
                // be removed unless it's also the first. Bypass it now that a new last
                // node has been enqueued.
                cNextHandle.setRelease(pp, node);
                // Return a more correct previous node, although it might be stale. Node
                // removal is somewhat lazy, and accurate removal is performed when the
                // exclusive latch is released.
                prev = pp;
            }
        }

        return prev;
    }

    /**
     * Should only be called after clearing the mWaiter field. Ideally, multiple threads
     * shouldn't be calling this method, because it can cause nodes to be resurrected and
     * remain in the queue longer than necessary. They'll get cleaned out eventually. The
     * problem is caused by the prev node reference, which might have changed or have been
     * removed by the time this method is called.
     *
     * @param node node to remove, not null
     * @param prev previous node, not null
     */
    private void remove(final WaitNode node, final WaitNode prev) {
        WaitNode next = node.mNext;

        if (next == null) {
            // Removing the last node creates race conditions with enqueues. Instead, stash a
            // reference to the previous node and let the enqueue deal with it after a new node
            // has been enqueued.
            node.mPrev = prev;
            next = node.mNext;
            // Double check in case an enqueue just occurred that may have failed to notice the
            // previous node assignment.
            if (next == null) {
                return;
            }
        }

        while (next.mWaiter == null) {
            // Skip more nodes if possible.
            WaitNode nextNext = next.mNext;
            if (nextNext == null) {
                break;
            }
            next = nextNext;
        }

        // Bypass the removed node, allowing it to be released.
        cNextHandle.setRelease(prev, next);
    }

    private WaitNode first() {
        int trials = 0;
        while (true) {
            WaitNode last = mLatchLast;
            if (last == null) {
                return null;
            }
            // Although the last waiter has been observed to exist, the first waiter field
            // might not be set yet.
            WaitNode first = mLatchFirst;
            if (first != null) {
                return first;
            }
            trials = spin(trials);
        }
    }

    /**
     * Returns the first waiter in the queue that's actually still waiting.
     */
    private WaitNode firstWaiter() {
        WaitNode first = mLatchFirst;
        WaitNode next;
        if (first == null || first.mWaiter != null || (next = first.mNext) == null) {
            return first;
        }
        if (next.mWaiter != null) {
            return next;
        }
        // Clean out some stale nodes. Note that removing the first node isn't safe.
        remove(next, first);
        return null;
    }

    public final boolean hasQueuedThreads() {
        return mLatchLast != null;
    }

    @Override
    public String toString() {
        var b = new StringBuilder();
        appendMiniString(b, this);
        b.append(" {state=");

        int state = mLatchState;
        if (state == 0) {
            b.append("unlatched");
        } else if (state == EXCLUSIVE) {
            b.append("exclusive");
        } else if (state >= 0) {
            b.append("shared:").append(state);
        } else {
            b.append("illegal:").append(state);
        }

        WaitNode last = mLatchLast;

        if (last != null) {
            b.append(", ");
            WaitNode first = mLatchFirst;
            if (first == last) {
                b.append("firstQueued=").append(last);
            } else if (first == null) {
                b.append("lastQueued=").append(last);
            } else {
                b.append("firstQueued=").append(first)
                    .append(", lastQueued=").append(last);
            }
        }

        return b.append('}').toString();
    }

    static void appendMiniString(StringBuilder b, Object obj) {
        if (obj == null) {
            b.append("null");
            return;
        }
        b.append(obj.getClass().getName()).append('@').append(Integer.toHexString(obj.hashCode()));
    }

    /**
     * @return new trials value
     */
    static int spin(int trials) {
        trials++;
        if (trials >= SPIN_LIMIT) {
            Thread.yield();
            trials = 0;
        } else {
            Thread.onSpinWait();
        }
        return trials;
    }

    static class WaitNode {
        volatile Object mWaiter;

        static final int SIGNALED = 1, COND_WAIT = 2, COND_WAIT_SHARED = 3;
        volatile int mWaitState;

        // Only set if node was deleted and must be bypassed when a new node is enqueued.
        volatile WaitNode mPrev;
        volatile WaitNode mNext;

        /**
         * Constructor for latch wait.
         */
        WaitNode() {
        }

        /**
         * Constructor for condition wait. Caller must hold exclusive latch.
         */
        WaitNode(Object waiter, int waitState) {
            cWaiterHandle.set(this, waiter);
            cWaitStateHandle.set(this, waitState);
        }

        /**
         * @return true if timed out or interrupted
         */
        boolean parkNow(Latch latch) {
            LockSupport.park(latch);
            return false;
        }

        /**
         * @return {@literal <0 if thread should park; 0 if acquired and node should also be
         * removed; >0 if acquired and node should not be removed}
         */
        int tryAcquire(Latch latch) {
            int trials = 0;
            while (true) {
                for (int i=0; i<SPIN_LIMIT; i++) {
                    boolean acquired = latch.doTryAcquireExclusive();
                    Object waiter = mWaiter;
                    if (waiter == null) {
                        // Fair handoff, and so node is no longer in the queue.
                        return 1;
                    }
                    if (acquired) {
                        // Acquired, so no need to reference the waiter anymore.
                        if (((int) cWaitStateHandle.get(this)) != SIGNALED) {
                            cWaiterHandle.setOpaque(this, null);
                        } else if (!cWaiterHandle.compareAndSet(this, waiter, null)) {
                            return 1;
                        }
                        return 0;
                    }
                    Thread.onSpinWait();
                }
                if (++trials >= SPIN_LIMIT >> 1 || timedOut()) {
                    return -1;
                }
                // Yield to avoid parking.
                Thread.yield();
            }
        }

        protected boolean timedOut() {
            return false;
        }

        @Override
        public String toString() {
            var b = new StringBuilder();
            appendMiniString(b, this);
            b.append(" {waiter=").append(mWaiter);
            b.append(", state=").append(mWaitState);
            b.append(", next="); appendMiniString(b, mNext);
            b.append(", prev="); appendMiniString(b, mPrev);
            return b.append('}').toString();
        }
    }

    static class Timed extends WaitNode {
        private long mNanosTimeout;
        private long mEndNanos;

        Timed(long nanosTimeout) {
            mNanosTimeout = nanosTimeout;
            if (nanosTimeout >= 0) {
                mEndNanos = System.nanoTime() + nanosTimeout;
            }
        }

        @Override
        final boolean parkNow(Latch latch) {
            if (mNanosTimeout < 0) {
                LockSupport.park(latch);
                return Thread.currentThread().isInterrupted();
            } else {
                LockSupport.parkNanos(latch, mNanosTimeout);
                if (Thread.currentThread().isInterrupted()) {
                    return true;
                }
                return (mNanosTimeout = mEndNanos - System.nanoTime()) <= 0;
            }
        }

        @Override
        protected boolean timedOut() {
            if (mNanosTimeout >= 0) {
                long timeout = mEndNanos - System.nanoTime();
                if (timeout <= 0) {
                    mNanosTimeout = 0;
                    return true;
                }
                mNanosTimeout = timeout;
            }
            return false;
        }
    }

    static class Shared extends WaitNode {
        /**
         * @return {@literal <0 if thread should park; 0 if acquired and node should also be
         * removed; >0 if acquired and node should not be removed}
         */
        @Override
        final int tryAcquire(Latch latch) {
            // Note: If mWaiter is null, then handoff was fair. The shared count should already
            // be correct, and this node won't be in the queue anymore.

            WaitNode first = latch.firstWaiter();
            if (first != null && !(first instanceof Shared)) {
                return mWaiter == null ? 1 : -1;
            }

            int trials = 0;
            while (true) {
                if (mWaiter == null) {
                    return 1;
                }

                int state = latch.mLatchState;
                if (state < 0) {
                    return state;
                }

                if (cStateHandle.compareAndSet(latch, state, state + 1)) {
                    // Acquired, so no need to reference the thread anymore.
                    Object waiter = mWaiter;
                    if (waiter == null || !cWaiterHandle.compareAndSet(this, waiter, null)) {
                        if (!cStateHandle.compareAndSet(latch, state + 1, state)) {
                            cStateHandle.getAndAdd(latch, -1);
                        }
                        return 1;
                    }

                    // Only instruct the caller to remove this node if this is the first shared
                    // latch owner (the returned state value will be 0). This guarantees that
                    // no other thread will be concurrently calling removeFirst. The node will
                    // be removed after an exclusive latch is released, or when firstWaiter is
                    // called again. Note that it's possible to return 0 every time, but only
                    // if the caller is also instructed to never call removeFirst.
                    return state;
                }

                trials = spin(trials);
            }
        }
    }

    static class TimedShared extends Shared {
        private long mNanosTimeout;
        private long mEndNanos;

        TimedShared(long nanosTimeout) {
            mNanosTimeout = nanosTimeout;
            if (nanosTimeout >= 0) {
                mEndNanos = System.nanoTime() + nanosTimeout;
            }
        }

        @Override
        final boolean parkNow(Latch latch) {
            if (mNanosTimeout < 0) {
                LockSupport.park(latch);
                return Thread.currentThread().isInterrupted();
            } else {
                LockSupport.parkNanos(latch, mNanosTimeout);
                if (Thread.currentThread().isInterrupted()) {
                    return true;
                }
                return (mNanosTimeout = mEndNanos - System.nanoTime()) <= 0;
            }
        }
    }
}
