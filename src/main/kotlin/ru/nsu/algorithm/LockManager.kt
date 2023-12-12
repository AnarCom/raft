package ru.nsu.algorithm

import java.time.Instant
import java.time.temporal.ChronoUnit

class LockManager(
    private val logJournal: LogJournal,
    private val raftState: RaftState,
    private val stateMachine: StateMachine,
) {
    private var lockAcquired = false
        set(value) {
            if (value != field) {
                println("isLockMy = $value")
                field = value
            }
            if (!value) {
                releasingLock = false
            }
        }

    private var timeToDeleteKey = 0L
        set(value) {
            if (field != value) {
                field = value
                println("Time to lock release by timeout $field/$lockTimeoutSec")
            }
        }

    private val lockTimeoutSec = 10L
    private var releasingLock = false

    fun getLockName() = "${raftState.self.host}|${raftState.self.port}"
    fun getLockNameWithUntilTime() = getLockName() + "|${Instant.now().epochSecond}"
    private fun isLockMy(): Boolean =
        logJournal.isLogActual() && (stateMachine.getFromState("locked_by")?.split("|")?.let {
            it.first() == raftState.self.host && it[1] == raftState.self.port.toString()
        } ?: false)


    fun getNextUntilTime() = Instant.now().plusSeconds(2 * lockTimeoutSec)
    fun lockLogic() {
        lockAcquired = isLockMy()
        if (raftState.state == NodeState.LEADER && stateMachine.getFromState("locked_by") != null) {
            val lockUntil = stateMachine.getFromState("locked_by")?.split("|")?.lastOrNull()?.toLong()
            if (lockUntil != null) {
                timeToDeleteKey = ChronoUnit.SECONDS.between(Instant.ofEpochSecond(lockUntil), Instant.now())
                if (timeToDeleteKey > lockTimeoutSec) {
                    logJournal.addEntity(
                        "delete_cas",
                        listOf("locked_by", stateMachine.getFromState("locked_by") ?: "")
                    )
                }
            }
        }

        if (lockAcquired && !releasingLock) {
            val lockUntil = stateMachine.getFromState("locked_by")?.split("|")?.lastOrNull()?.toLong()
            if (lockUntil != null) {
                if (ChronoUnit.SECONDS.between(Instant.ofEpochSecond(lockUntil), Instant.now()) > lockTimeoutSec / 2) {
                    logJournal.addEntity(
                        "cas",
                        listOf("locked_by", getLockNameWithUntilTime(), stateMachine.getFromState("locked_by") ?: "")
                    )
                }
            }
        }
        if (lockAcquired && releasingLock) {
            lastLockValue = stateMachine.getFromState("locked_by") ?: ""
            release()
        }
    }

    private var lastLockValue: String = ""

    fun release() {
        if (lockAcquired) {
            releasingLock = true
        }
        logJournal.addEntity(
            "delete_cas",
            listOf("locked_by", lastLockValue)
        )
        println("adding lock")
    }
}
