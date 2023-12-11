package ru.nsu.algorithm

import ru.nsu.dto.NodeInformation
import java.time.Instant
import java.time.temporal.ChronoUnit

class RaftState(
    val self: NodeInformation
) {
    var timeStarted = Instant.now()
    var term: ULong = 0U
    lateinit var leader: NodeInformation

    var electedFor: NodeInformation? = null
    var votes = 0

    private val ELECTION_TIMEOUT_MS = 1000
    var state: NodeState = NodeState.FOLLOWER
        set(value) {
            resetTime()
            if(value != field) {
                println(value)
            }
            field = value
        }

    fun resetTime() {
        timeStarted = Instant.now()
    }

    fun isLeaderDead() = ChronoUnit.MILLIS.between(timeStarted, Instant.now()) > ELECTION_TIMEOUT_MS
    fun incTerm(): ULong {
        electedFor = null
        votes = 0
        return ++term
    }
    fun isElectionTimeout() = ChronoUnit.MILLIS.between(timeStarted, Instant.now()) > ELECTION_TIMEOUT_MS
    fun isTimeToSendHeartBeat() = ChronoUnit.MILLIS.between(timeStarted, Instant.now()) > (ELECTION_TIMEOUT_MS / 10)
}
