package zip.trev.trevrpc.cronet

import kotlinx.coroutines.CancellableContinuation
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import java.nio.ByteBuffer

/**
 * Where one call stands logically, whatever its native Cronet stream is doing. A call only moves
 * forward: [Open], then optionally [RemoteTerminal], then [Settled]. [CallStateMachine] owns the
 * transitions.
 */
internal sealed interface CallState {
    /** The server's terminal status, or `null` while the call is open or when it ended without one. */
    val remoteStatus: Status?

    /** The call is running: the client may send and Cronet's callbacks are acted on. */
    data object Open : CallState {
        override val remoteStatus: Status? get() = null
    }

    /** A call that is over for sending and for Cronet's callbacks, though possibly not yet settled. */
    sealed interface Ended : CallState {
        /** Why a send on this call fails. */
        fun sendError(): TrevRpcException
    }

    /**
     * The server's terminal STATUS was decoded. Responses still drain to the reader, so the call is
     * not [Settled] yet, but nothing more can be sent and Cronet's callbacks no longer matter.
     */
    class RemoteTerminal(
        val status: Status,
    ) : Ended {
        override val remoteStatus: Status get() = status

        override fun sendError(): TrevRpcException = remoteSendError(status)
    }

    /**
     * The call is finished and its streams are closed: with [cause] when the transport, a protocol
     * violation or the caller ended it, or with `null` after a plain close or the server's end of
     * response. [remoteStatus] is the server's STATUS when one was decoded first.
     */
    class Settled(
        val cause: Throwable?,
        override val remoteStatus: Status?,
    ) : Ended {
        /**
         * Why a send fails once the call is over: the server's status when it ended the call, else
         * the transport or protocol error that ended it, else a plain close.
         */
        override fun sendError(): TrevRpcException {
            remoteStatus?.let { return remoteSendError(it) }
            return if (cause is TrevRpcException) {
                TrevRpcException(cause.status, cause)
            } else {
                TrevRpcException(Status.cancelled("request stream is closed"), cause)
            }
        }
    }
}

/**
 * A send's one write in flight: its direct [buffer], whether it half-closes, and the [continuation]
 * the send suspends on until Cronet reports the write complete.
 */
internal class PendingWrite(
    val buffer: ByteBuffer,
    val endOfStream: Boolean,
    val continuation: CancellableContinuation<Unit>,
)

/**
 * A [CallState] transition that took effect, with the write that was in flight when it did. The
 * transition took the write, so the caller owns resuming its continuation.
 */
internal class Transition<S : CallState>(
    val state: S,
    val pendingWrite: PendingWrite?,
)

/**
 * The logical call state and the one write in flight, guarded by a single lock so every transition
 * out of [CallState.Open] takes the pending write in the same critical section. The state is
 * readable without the lock: callbacks on Cronet's hot paths only compare it with [CallState.Open].
 *
 * Invariant: a write is pending only while the call is [CallState.Open]. A write completion that
 * finds no matching pending write on an open call is therefore genuinely unexpected; one that finds
 * the call over belongs to a write a transition has already taken.
 */
internal class CallStateMachine {
    private val lock = Any()
    private var pendingWrite: PendingWrite? = null

    @Volatile
    var state: CallState = CallState.Open
        private set

    /**
     * Installs [pending] as the write in flight. Returns `null` when installed, or the state that
     * refused it: an ended call, or [CallState.Open] when the send was already canceled.
     */
    fun installWrite(pending: PendingWrite): CallState? =
        synchronized(lock) {
            val current = state
            if (current !is CallState.Open || !pending.continuation.isActive) return current
            check(pendingWrite == null) { "only one Cronet write may be in flight" }
            pendingWrite = pending
            null
        }

    /** Whether [pending] is still the write in flight, so the call is still open. */
    fun isPending(pending: PendingWrite): Boolean = synchronized(lock) { pendingWrite === pending }

    /**
     * Takes the write in flight when it is the one Cronet reports complete. A `null` result with the
     * call still open means Cronet completed a write that was never issued.
     */
    fun takeCompletedWrite(
        buffer: ByteBuffer,
        endOfStream: Boolean,
    ): PendingWrite? =
        synchronized(lock) {
            val pending = pendingWrite
            if (pending == null || pending.buffer !== buffer || pending.endOfStream != endOfStream) return null
            pendingWrite = null
            pending
        }

    /** Moves an [CallState.Open] call to [CallState.RemoteTerminal], taking the write in flight. */
    fun beginRemoteTerminal(status: Status): Transition<CallState.RemoteTerminal>? =
        synchronized(lock) {
            if (state !is CallState.Open) return null
            advance(CallState.RemoteTerminal(status))
        }

    /** Settles the call from any state but [CallState.Settled], taking the write in flight. */
    fun settle(cause: Throwable?): Transition<CallState.Settled>? = synchronized(lock) { settleLocked(cause) }

    /** Settles the call only while it is [CallState.Open]. */
    fun settleIfOpen(cause: Throwable?): Transition<CallState.Settled>? =
        synchronized(lock) { if (state is CallState.Open) settleLocked(cause) else null }

    /**
     * Settles the call because the write [pending] failed or was canceled, unless a transition or
     * the write's completion already took it. Clearing the write and settling are one step, so no
     * completion can see the write gone while the call still looks open.
     */
    fun settleWrite(
        pending: PendingWrite,
        cause: Throwable,
    ): Transition<CallState.Settled>? = synchronized(lock) { if (pendingWrite === pending) settleLocked(cause) else null }

    private fun settleLocked(cause: Throwable?): Transition<CallState.Settled>? {
        val current = state
        if (current is CallState.Settled) return null
        return advance(CallState.Settled(cause, current.remoteStatus))
    }

    private fun <S : CallState> advance(next: S): Transition<S> {
        val pending = pendingWrite
        pendingWrite = null
        state = next
        return Transition(next, pending)
    }
}

private fun remoteSendError(status: Status): TrevRpcException =
    if (status.isOk) {
        TrevRpcException(Status.cancelled("the server completed the call before the request stream finished"))
    } else {
        TrevRpcException(status)
    }

/**
 * The native Cronet stream's lifecycle, with the cancel request folded in: `Unopened`, `Starting`,
 * `Started` and `Terminal`. Provider calls (`start`, `cancel`) happen outside the lock, on the
 * exchange's behalf, driven by what these methods return.
 *
 * - A cancel requested before the stream is started is remembered and issued the moment `start()`
 *   returns, because a Cronet stream cannot be canceled while it is starting.
 * - Cancel is issued at most once, and never after a terminal callback.
 * - A terminal callback that arrives while `start()` is running defers the lease release until
 *   `start()` returns, so the exchange never settles while a provider call is on the stack.
 * - Each of [beginStart] (refused), [startFailed], [startReturned] and [terminated] that reports
 *   the native stream done does so once; the exchange releases its lease then.
 */
internal class NativeStreamLifecycle {
    private val lock = Any()
    private var phase: Phase = Phase.Unopened(cancelRequested = false)

    /** What the exchange does once `start()` returned. */
    enum class AfterStart {
        /** Nothing more: the stream runs. */
        RUNNING,

        /** The stream reached a terminal callback during `start()`: release the lease. */
        FINISHED,

        /** A cancel was requested during `start()`: issue it now. */
        CANCEL,
    }

    private sealed interface Phase {
        /** The factory has not returned a stream, or has but `start()` was not begun. */
        data class Unopened(
            val cancelRequested: Boolean,
        ) : Phase

        /** `start()` is running. */
        data class Starting(
            val cancelPending: Boolean,
            val terminalSeen: Boolean,
        ) : Phase

        /** `start()` returned and no terminal callback came. */
        data class Started(
            val cancelIssued: Boolean,
        ) : Phase

        /** Nothing is left to start, cancel or wait for: the native side is done. */
        data object Terminal : Phase
    }

    /**
     * Returns whether to call the provider's `start()`. `false` means a cancel was requested while the
     * stream was not yet open, so it is abandoned unstarted and the caller releases its lease.
     */
    fun beginStart(): Boolean =
        synchronized(lock) {
            val current = phase as Phase.Unopened
            if (current.cancelRequested) {
                phase = Phase.Terminal
                false
            } else {
                phase = Phase.Starting(cancelPending = false, terminalSeen = false)
                true
            }
        }

    /** `start()` threw: nothing was started, so there is nothing to cancel. The caller releases its lease. */
    fun startFailed() {
        synchronized(lock) { phase = Phase.Terminal }
    }

    /** `start()` returned. */
    fun startReturned(): AfterStart =
        synchronized(lock) {
            val current = phase as Phase.Starting
            when {
                current.terminalSeen -> {
                    phase = Phase.Terminal
                    AfterStart.FINISHED
                }

                current.cancelPending -> {
                    phase = Phase.Started(cancelIssued = true)
                    AfterStart.CANCEL
                }

                else -> {
                    phase = Phase.Started(cancelIssued = false)
                    AfterStart.RUNNING
                }
            }
        }

    /** Returns whether the caller must issue the provider cancel now; at most once over the lifecycle. */
    fun requestCancel(): Boolean =
        synchronized(lock) {
            when (val current = phase) {
                is Phase.Unopened -> {
                    phase = current.copy(cancelRequested = true)
                    false
                }

                is Phase.Starting -> {
                    phase = current.copy(cancelPending = true)
                    false
                }

                is Phase.Started -> {
                    if (current.cancelIssued) {
                        false
                    } else {
                        phase = Phase.Started(cancelIssued = true)
                        true
                    }
                }

                Phase.Terminal -> {
                    false
                }
            }
        }

    /**
     * A terminal callback (succeeded, failed or canceled) arrived. Returns whether the caller
     * releases its lease now; during `start()` the release waits for [startReturned].
     */
    fun terminated(): Boolean =
        synchronized(lock) {
            when (val current = phase) {
                is Phase.Starting -> {
                    phase = current.copy(terminalSeen = true)
                    false
                }

                is Phase.Started -> {
                    phase = Phase.Terminal
                    true
                }

                is Phase.Unopened, Phase.Terminal -> {
                    false
                }
            }
        }
}
