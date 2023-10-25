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
    JsonSubTypes.Type(value = VoteRequest::class, name = "vote_request"),
    JsonSubTypes.Type(value = VoteAnswer::class, name = "vote_answer"),
    JsonSubTypes.Type(value = HeartBeat::class, name = "heart_beat"),
)
open class JsonMessage(
    val nodeIdentification: Int,
    val epochCount: ULong
) {
    @JsonCreator
    constructor() : this(0, 0U)
}


class VoteRequest(
    nodeIdentification: Int,
    epochCount: ULong
) : JsonMessage(nodeIdentification, epochCount) {
    @JsonCreator
    constructor() : this(0, 0U)
}

class VoteAnswer(
    nodeIdentification: Int,
    epochCount: ULong,
    val voteResult: Boolean
) : JsonMessage(nodeIdentification, epochCount) {
    @JsonCreator
    constructor() : this(0, 0U, false)
}

class HeartBeat(
    nodeIdentification: Int,
    epochCount: ULong
) : JsonMessage(nodeIdentification, epochCount) {
    @JsonCreator
    constructor() : this(0, 0U)
}

