import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes(
    JsonSubTypes.Type(value = JsonMessage::class, name = "hand_shake"),
    JsonSubTypes.Type(value = MessageWithEpoch::class, name = "with_epoch"),
    JsonSubTypes.Type(value = VoteRequest::class, name = "vote_request"),
    JsonSubTypes.Type(value = VoteAnswer::class, name = "vote_answer"),
    JsonSubTypes.Type(value = HeartBeat::class, name = "heart_beat"),
    JsonSubTypes.Type(value = RegisterLog::class, name = "register_log"),
    JsonSubTypes.Type(value = CasRequest::class, name = "cas_request")
)
open class JsonMessage(
    val nodeIdentification: Int,
) {
    @JsonCreator
    constructor() : this(0)
}

open class MessageWithEpoch(
    nodeIdentification: Int,
    val epochCount: ULong
) : JsonMessage(nodeIdentification) {
    @JsonCreator
    constructor() : this(0, 0U)
}

class VoteRequest(
    nodeIdentification: Int,
    epochCount: ULong
) : MessageWithEpoch(nodeIdentification, epochCount) {
    @JsonCreator
    constructor() : this(0, 0U)
}

class VoteAnswer(
    nodeIdentification: Int,
    epochCount: ULong,
    val voteResult: Boolean
) : MessageWithEpoch(nodeIdentification, epochCount) {
    @JsonCreator
    constructor() : this(0, 0U, false)
}

class HeartBeat(
    nodeIdentification: Int,
    epochCount: ULong
) : MessageWithEpoch(nodeIdentification, epochCount) {
    @JsonCreator
    constructor() : this(0, 0U)
}

class RegisterLog(
    nodeIdentification: Int,
    epochCount: ULong,
    val key: String,
    val value: String,
) : MessageWithEpoch(nodeIdentification, epochCount) {
    @JsonCreator
    constructor() : this(0, 0U, "", "")
}

class CasRequest(
    nodeIdentification: Int,
    epochCount: ULong,
    val key: String,
    val value: String,
    val expectedValue: String,
) : MessageWithEpoch(nodeIdentification, epochCount) {
    @JsonCreator
    constructor() : this(0, 0U, "", "", "")
}
