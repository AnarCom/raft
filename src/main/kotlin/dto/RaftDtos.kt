package dto

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

@Serializable
class NodeInformation(
    val port: Int,
    val host: String,
) {
    override fun toString(): String {
        return "NodeInformation(port=$port, host='$host')"
    }
}

@Serializable
sealed class BaseMessage {
    abstract val nodeInformation: NodeInformation
    abstract val term: ULong
}

@Serializable
class HandShake(
    override val nodeInformation: NodeInformation,
    override val term: ULong
) : BaseMessage()

@Serializable
class VoteRequest(
    override val nodeInformation: NodeInformation,
    override val term: ULong,
    val lastLogIndex: Int,
    val lastLogTerm: ULong
) : BaseMessage()

@Serializable
class VoteResponse(
    override val nodeInformation: NodeInformation,
    override val term: ULong,
    val answer: Boolean
) : BaseMessage()

@Serializable
class LogEntryDto(
    val command: String,
    val arguments: List<String>,
    val term: ULong
)

@Serializable
class HeartBeatRequest(
    override val nodeInformation: NodeInformation,
    override val term: ULong,
    val prevLogIndex: Int,
    val prevLogTerm: ULong,
    val entries: List<LogEntryDto>,
    val leaderCommitIndex: Int,
) : BaseMessage()

@Serializable
class HeartBeatResponse(
    override val nodeInformation: NodeInformation,
    override val term: ULong,
    val success: Boolean
) : BaseMessage()

fun serialize(t: BaseMessage) = Json.encodeToString(BaseMessage.serializer(), t)
