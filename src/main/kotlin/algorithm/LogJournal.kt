package algorithm

import dto.HeartBeatRequest
import dto.LogEntryDto
import dto.NodeInformation
import kotlin.math.min

class LogJournal(
    private val raftState: RaftState,
) {
    private val log: MutableList<LogEntry> = mutableListOf()

    private data class LogEntry(
        val command: String,
        val arguments: List<String>,
        val term: ULong,
        val leader: NodeInformation,
        var isCommitted: Boolean = false,
    ) {
        override fun toString(): String {
            return "LogEntry(" +
                    "command='$command', " +
                    "arguments=$arguments, " +
                    "term=$term, " +
                    "leader=$leader, " +
                    "isCommitted=$isCommitted)\n"
        }
    }

    fun appendEntities(request: HeartBeatRequest): Boolean {
        // Reply false if term < currentTerm
        if (request.term < raftState.term) {
            return false
        }

        // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
        if (
            log.size < request.prevLogIndex ||
            log[request.prevLogIndex - 1].term != request.prevLogTerm
        ) {
            return false
        }

        if (log.size > request.prevLogIndex + request.entries.size) {
            log.subList(request.prevLogIndex + request.entries.size, log.size).clear()
        }

        log.addAll(
            request.entries.map {
                LogEntry(
                    it.command,
                    it.arguments,
                    it.term,
                    request.nodeInformation,
                    isCommitted = false
                )
            }
        )

        log.forEachIndexed { index, logEntry ->
            if (index < request.leaderCommitIndex) {
                logEntry.isCommitted = true
            }
        }

        return true
    }

    private fun getCommitIndex() = log.takeWhile { it.isCommitted }.size
    fun createRequest(
        self: NodeInformation,
        nodeIndex: Int,
    ) = log.slice(nodeIndex..<min(log.size, nodeIndex + 5)).toList().map {
        LogEntryDto(
            it.command,
            it.arguments,
            it.term
        )
    }.let {
        HeartBeatRequest(
            self,
            raftState.term,
            nodeIndex,
            log[nodeIndex-1].term,
            it,
            getCommitIndex()
        )
    }

    fun getLastLogIndex() = log.size

    fun getLastLogTerm(): ULong = getLastLogIndex().let {
        if (it == 0) {
            return 0U
        } else {
            return log[it - 1].term
        }
    }
}
