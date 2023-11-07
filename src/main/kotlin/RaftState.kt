import java.time.Instant
import java.time.temporal.ChronoUnit

class RaftState(
    var nodeState: ProgramState = ProgramState.FOLLOWER,
    var termNumber: ULong = 0U,
    var termStarted: Instant = Instant.now(),
) {
    val ELECTION_TIMEOUT_MS = 1000
    fun incTerm() = ++termNumber
    fun isLeaderDead() = ChronoUnit.MILLIS.between(termStarted, Instant.now()) > ELECTION_TIMEOUT_MS
    fun isElectionTimeout() = ChronoUnit.MILLIS.between(termStarted, Instant.now()) > ELECTION_TIMEOUT_MS
    fun isTimeToSendHeartBeat() = ChronoUnit.MILLIS.between(termStarted, Instant.now()) > (ELECTION_TIMEOUT_MS / 10)

}
