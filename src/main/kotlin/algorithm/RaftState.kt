package algorithm

import java.time.Instant
import java.time.temporal.ChronoUnit

class RaftState(
    var state: NodeState = NodeState.FOLLOWER,
) {
    var timeStarted = Instant.now()
    var term: ULong = 0U
    private val ELECTION_TIMEOUT_MS = 1000

    fun resetTime() {
        timeStarted = Instant.now()
    }

    fun isLeaderDead() = ChronoUnit.MILLIS.between(timeStarted, Instant.now()) > ELECTION_TIMEOUT_MS
    fun incTerm() = ++term
    fun isElectionTimeout() = ChronoUnit.MILLIS.between(timeStarted, Instant.now()) > ELECTION_TIMEOUT_MS
    fun isTimeToSendHeartBeat() = ChronoUnit.MILLIS.between(timeStarted, Instant.now()) > (ELECTION_TIMEOUT_MS / 10)
}
