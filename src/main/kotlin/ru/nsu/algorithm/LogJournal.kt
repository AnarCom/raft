package ru.nsu.algorithm

import kotlinx.serialization.Serializable
import ru.nsu.dto.HeartBeatRequest
import ru.nsu.dto.LogEntryDto
import ru.nsu.dto.NodeInformation
import kotlin.math.min

class LogJournal(
    private val raftState: RaftState,
    private val stateMachine: StateMachine,
) {
    private val log: MutableList<LogEntry> = mutableListOf()
    val transferToMaster: MutableList<Pair<String, List<String>>> = mutableListOf()

    @Serializable
    data class LogEntry(
        val command: String,
        val arguments: List<String>,
        val term: ULong,
        val leader: NodeInformation,
        var isCommitted: Boolean = false,
        val set: MutableSet<Pair<String, Int>> = mutableSetOf()
    ) {
        override fun toString(): String {
            return "LogEntry(command='$command', " +
                    "arguments=$arguments, " +
                    "term=$term, " +
                    "leader=$leader, " +
                    "isCommitted=$isCommitted, " +
                    "set=$set)"
        }
    }

    private var isLogActual = true
    fun isLogActual() = raftState.state == NodeState.LEADER || isLogActual
    fun addEntity(
        command: String,
        arguments: List<String>
    ) {
        if (raftState.state != NodeState.LEADER) {
            transferToMaster.add(Pair(command, arguments))
        } else {
            log.add(
                LogEntry(
                    command,
                    arguments,
                    raftState.term,
                    raftState.self
                )
            )
        }
    }

    fun lsJournal() = log.toList()

    fun appendEntities(request: HeartBeatRequest): Boolean {
        // Reply false if term < currentTerm
        if (request.term < raftState.term) {
            return false
        }

        // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
        if (
            log.size < request.prevLogIndex ||
            (request.prevLogIndex > 0 &&
                    log[request.prevLogIndex - 1].term != request.prevLogTerm)
        ) {
            return false
        }

        if (log.size > request.prevLogIndex) {
            log.subList(request.prevLogIndex, log.size).clear()
        }

        log.addAll(request.entries.map {
            LogEntry(
                it.command, it.arguments, it.term, request.nodeInformation, isCommitted = false
            )
        })

        log.forEachIndexed { index, logEntry ->
            if (index < request.leaderCommitIndex && !logEntry.isCommitted) {
                logEntry.isCommitted = true
                stateMachine.applyState(logEntry.command, logEntry.arguments)
            }
        }
        return true
    }

    private fun getCommitIndex() = log.takeWhile { it.isCommitted }.size
    fun createRequest(
        nodeIndex: Int,
    ): Pair<HeartBeatRequest, Int> {
        val res = log.slice(nodeIndex..<min(log.size, nodeIndex + 5)).toList().map {
            LogEntryDto(
                it.command, it.arguments, it.term
            )
        }.let {
            Pair(
                HeartBeatRequest(
                    raftState.self, raftState.term, nodeIndex, getLastLogTerm(nodeIndex), it, getCommitIndex()
                ),
                min(log.size, nodeIndex + 5)
            )
        }
        return res
    }


    fun getLastLogIndex() = log.size

    fun getLastLogTerm(nodeIndex: Int? = null): ULong = (nodeIndex ?: getLastLogIndex()).let {
        if (it == 0) {
            return 0U
        } else {
            return log[it - 1].term
        }
    }

    fun processResponse(host: String, port: Int, dueIndex: Int, quota: Int) {
        log.forEachIndexed { index, logEntry ->
            if (!logEntry.isCommitted || index >= dueIndex) {
                logEntry.set.add(Pair(host, port))
                if (logEntry.set.size + 1 >= quota) {
                    logEntry.set.clear()
                    logEntry.isCommitted = true
                    stateMachine.applyState(logEntry.command, logEntry.arguments)
                }
            }
        }
    }


    fun clearLog() {
        println("test command")
        log.clear()
        println("log size is not ${log.size}")
    }
}
