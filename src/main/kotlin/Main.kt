import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import utils.formatHexDump
import utils.getJackson
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.Exception
import kotlin.concurrent.thread
import kotlin.math.ceil

const val ELECTION_TIMEOUT_MS = 1000

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

enum class ProgramState {
    FOLLOWER,
    CANDIDATE,
    LEADER
}

fun connectToAll(currentPort: Int, programCount: Int, selector: Selector, connections: MutableMap<Int, SocketChannel>) {
    (10_000..<10_000 + programCount)
        .filter {
            currentPort != it
        }
        .map {
            thread {
                val mapper = getJackson()
                println("connecting to $it")
                val client = SocketChannel.open(InetSocketAddress("localhost", it))
                client.configureBlocking(false)
//                client.write(ByteBuffer.wrap(mapper.writeValueAsBytes(JsonMessage(currentPort))))
                writeToSocketChanel(client, JsonMessage(currentPort), mapper)
                val key = client.register(selector, SelectionKey.OP_READ)
                key.attach(it)
                synchronized(connections) {
                    connections[it] = client
                }
            }
        }
        .map {
            try {
                it.join()
            } catch (_: Exception) {
            }
        }
}

fun sendLogToAll(
    connections: Map<Int, SocketChannel>,
    data: Pair<String, String>,
    nodeIdentification: Int,
    epochCount: ULong,
    mapper: ObjectMapper
) {
    println(connections.keys)
    connections.values.forEach {
        writeToSocketChanel(it, RegisterLog(nodeIdentification, epochCount, data.first, data.second), mapper)
//        it.write(
//            ByteBuffer.wrap(
//                mapper.writeValueAsBytes(RegisterLog(nodeIdentification, epochCount, data.first, data.second))
//            )
//        )
    }
}

//fun readFromSocket(connection: SocketChannel, mapper: ObjectMapper): JsonMessage? = try {
//    val byteBuffer = ByteBuffer.allocate(1024)
//    val read = connection.read(byteBuffer)
//    if (read == -1) {
//        null
//    } else {
//        byteBuffer.flip()
//        val json = String(byteBuffer.array().filter { it != 0.toByte() }.toByteArray())
//        mapper.readValue<JsonMessage>(json).apply {
//            if (this !is HeartBeat) {
//                println(json)
//            }
//        }
//    }
//} catch (e: Exception) {
//    null
//}
fun readFromSocket(connection: SocketChannel, mapper: ObjectMapper): JsonMessage? =
    try {
        val sizeBuffer = ByteBuffer.allocate(Int.SIZE_BYTES)
        if (connection.read(sizeBuffer) == -1) {
            null
        } else {
            sizeBuffer.flip()
            val size = sizeBuffer.getInt()
            val bufferForJson = ByteBuffer.allocate(size)
            connection.read(bufferForJson)
            bufferForJson.flip()
            val str = String(bufferForJson.array().filter { it != 0.toByte() }.toByteArray())
            mapper.readValue<JsonMessage>(str).apply {
                if(this !is HeartBeat) {
                    println(str)
                }
            }
        }
    } catch (e: Exception) {
        println("exception on reading: $e")
        null
    }

fun writeToSocketChanel(
    socketChannel: SocketChannel,
    data: JsonMessage,
    mapper: ObjectMapper,
) {
    try {
        val bytes = mapper.writeValueAsBytes(
            data
        )
        val buffer = ByteBuffer.allocate(Int.SIZE_BYTES + bytes.size).apply {
            putInt(bytes.size)
            put(bytes)
            flip()
        }
        if (data !is HeartBeat) {
            formatHexDump(buffer.array())
        }
        socketChannel.write(
            buffer
//            ByteBuffer.wrap(
//                mapper.writeValueAsBytes(
//                    data
//                )
//            )
        )
    } catch (e: Exception) {
        println("catch on sending: $e")
    }
}


val log = mutableListOf<Pair<String, String>>()
fun main(args: Array<String>) {

    val currentPort = args[1].toInt()
    val nPrograms = args[0].toInt()
    println("starting server")
    val selector = Selector.open()
    val serverSocketChanel = ServerSocketChannel.open()
    serverSocketChanel.bind(InetSocketAddress("localhost", currentPort))
    serverSocketChanel.configureBlocking(false)
    serverSocketChanel.register(selector, SelectionKey.OP_ACCEPT)
    println("server started")
    val connections = mutableMapOf<Int, SocketChannel>()
    val mapper = jacksonObjectMapper()
    connectToAll(currentPort, nPrograms, selector, connections)
    Thread.sleep(100)
    println("debug sleep finished")
    var epochStarted = Instant.now()
    var epochCounter: ULong = 0U
    var nodeState = ProgramState.FOLLOWER
    var master = 0
    // отказаться от этой мапы, исходя из того, что если эпоха больше, то мы еще не голосовали
    val isVoted: MutableMap<ULong, Boolean> = mutableMapOf()
    // if == -1 -> мы не запрашивали голосование -> игнорируем запросы
    var votesCounter = -1

    val bufferedReader = System.`in`.bufferedReader()
    val logCache = mutableMapOf<String, String>()

    while (true) {
        selector.select(100)
        val iterator = selector.selectedKeys().iterator()
        while (iterator.hasNext()) {
            val key = iterator.next()
            if (key.isAcceptable) {
                val client = serverSocketChanel.accept()
                client.configureBlocking(false)
                client.register(selector, SelectionKey.OP_READ)
                println("connected")
            } else if (key.isReadable) {
                val client = key.channel() as SocketChannel
                val messageData = readFromSocket(client, mapper)
//                val byteBuffer = ByteBuffer.allocate(1024)
//                val read = client.read(byteBuffer)
//                if (read == -1) {
                if (messageData == null) {
                    if (key.attachment() != null) {
                        println("detached ${key.attachment()}")
                        val port = key.attachment() as Int
                        connections.remove(port)
                    }
                    client.close()
                } else {
//                    byteBuffer.flip()

//                    val json = String(byteBuffer.array().filter { it != 0.toByte() }.toByteArray())
//                    println(json)
//                    when (val messageData = mapper.readValue<JsonMessage>(json)) {
                    when (messageData) {
                        is HeartBeat -> {
                            //TODO: check that
                            epochStarted = Instant.now()
                            master = messageData.nodeIdentification
                        }

                        is VoteRequest -> {
                            val voteResult = messageData.epochCount.let { messageEpoch ->
                                isVoted[epochCounter].let {
                                    if ((it == false || it == null) && epochCounter < messageEpoch) {
                                        isVoted[epochCounter] = true
                                        true
                                    } else {
                                        false
                                    }
                                }
                            }
                            writeToSocketChanel(
                                client,
                                VoteAnswer(
                                    currentPort,
                                    epochCounter,
                                    voteResult
                                ),
                                mapper
                            )
//                            client.write(
//                                ByteBuffer.wrap(
//                                    mapper.writeValueAsBytes(
//                                        VoteAnswer(
//                                            currentPort,
//                                            epochCounter,
//                                            voteResult
//                                        )
//                                    )
//                                )
//                            )
                            epochStarted = Instant.now()
                        }

                        is VoteAnswer -> {
                            if (votesCounter != -1 && messageData.voteResult) {
                                votesCounter++
                            }
                            println("quota ${ceil(nPrograms.toDouble() / 2.toDouble()).toInt()}")
                            if (votesCounter >= ceil(nPrograms.toDouble() / 2.toDouble()).toInt()) {
                                nodeState = ProgramState.LEADER
                                votesCounter = -1
                                epochStarted = Instant.now()
                                println("I am now leader")
                            }
                        }

                        is RegisterLog -> {
                            Pair(messageData.key, messageData.value).let {
                                //move operations into class
                                log.add(it)
                                logCache[it.first] = it.second
                                if (nodeState == ProgramState.LEADER) {
                                    sendLogToAll(connections, it, currentPort, epochCounter, mapper)
                                }
                            }
                        }

                        is CasRequest -> {
                            if (nodeState == ProgramState.LEADER) {
                                if (logCache[messageData.key] == messageData.expectedValue) {
                                    Pair(messageData.key, messageData.value).let {
                                        log.add(it)
                                        logCache[it.first] = it.second
                                        sendLogToAll(connections, it, currentPort, epochCounter, mapper)
                                    }
                                }
                            }
                        }

                        is JsonMessage -> {
                            println("get new connection from ${messageData.nodeIdentification}")
                            connections[messageData.nodeIdentification] = client
                            key.attach(messageData.nodeIdentification)
                            if (nodeState == ProgramState.LEADER) {
                                for (logEntry in log) {
                                    writeToSocketChanel(
                                        client,
                                        RegisterLog(currentPort, epochCounter, logEntry.first, logEntry.second),
                                        mapper
                                    )
//                                    client.write(
//                                        ByteBuffer.wrap(
//                                            mapper.writeValueAsBytes(
//                                                RegisterLog(currentPort, epochCounter, logEntry.first, logEntry.second)
//                                            )
//                                        )
//                                    )
                                }
                            }
                        }

                        else -> {
                            println("unsupported type")
                        }
                    }
                }
            }
            iterator.remove()
        }
        if (bufferedReader.ready()) {
            val command = bufferedReader.readLine().split(" ")
            println("readed")
            if (command.isNotEmpty()) {
                when (command[0]) {
                    "set" -> {
                        if (master > 100) {
                            connections[master]?.let {
                                writeToSocketChanel(
                                    it,
                                    RegisterLog(
                                        currentPort,
                                        epochCounter,
                                        command[1],
                                        command[2]
                                    ),
                                    mapper
                                )
                            }

//                            connections[master]!!.write(
//                                ByteBuffer.wrap(
//                                    mapper.writeValueAsBytes(
//                                        RegisterLog(
//                                            currentPort,
//                                            epochCounter,
//                                            command[1],
//                                            command[2]
//                                        )
//                                    )
//                                )
//                            )
                        } else if (nodeState == ProgramState.LEADER) {
                            Pair(command[1], command[2]).let {
                                log.add(it)
                                logCache[it.first] = it.second
                                sendLogToAll(connections, it, currentPort, epochCounter, mapper)
                            }
                            println("added")
                        }
                    }

                    "get" -> {
                        println("stored value: ${logCache[command[1]]}")
                    }

                    "cas" -> {
                        val key = command[1]
                        val oldValue = command[2]
                        val newValue = command[3]
                        if (nodeState == ProgramState.LEADER) {
                            if (logCache[key] == oldValue) {
                                Pair(key, newValue).let {
                                    logCache[key] = newValue
                                    log.add(it)
                                    sendLogToAll(connections, it, currentPort, epochCounter, mapper)
                                }
                            }
                        } else {
                            connections[master]?.let {
                                writeToSocketChanel(
                                    it,
                                    CasRequest(
                                        currentPort,
                                        epochCounter,
                                        key,
                                        newValue,
                                        oldValue
                                    ),
                                    mapper
                                )
                            }
//                            connections[master]!!.write(
//                                ByteBuffer.wrap(
//                                    mapper.writeValueAsBytes(
//                                        CasRequest(
//                                            currentPort,
//                                            epochCounter,
//                                            key,
//                                            newValue,
//                                            oldValue
//                                        )
//                                    )
//                                )
//                            )
                        }
                    }

                    else -> {
                        println("unknown command")
                    }
                }
            }
        }

        when (nodeState) {
            ProgramState.FOLLOWER -> {
                if (ChronoUnit.MILLIS.between(epochStarted, Instant.now()) > ELECTION_TIMEOUT_MS) {
                    if (connections.keys.size > 0) {
                        epochStarted = Instant.now()
                        nodeState = ProgramState.CANDIDATE
                        epochCounter++
                        connections.values.forEach {
                            writeToSocketChanel(
                                it,
                                VoteRequest(currentPort, epochCounter),
                                mapper
                            )
//                            it.write(ByteBuffer.wrap(mapper.writeValueAsBytes(VoteRequest(currentPort, epochCounter))))
                        }
                        votesCounter = 1
                    }
                }
            }

            ProgramState.CANDIDATE -> {
                if (ChronoUnit.MILLIS.between(epochStarted, Instant.now()) > ELECTION_TIMEOUT_MS) {
                    nodeState = ProgramState.FOLLOWER
                    votesCounter = -1
                }
            }

            ProgramState.LEADER -> {
                if (ChronoUnit.MILLIS.between(epochStarted, Instant.now()) > (ELECTION_TIMEOUT_MS / 10)) {
                    connections.values.forEach {
                        writeToSocketChanel(
                            it,
                            HeartBeat(currentPort, epochCounter),
                            mapper
                        )
//                        it.write(ByteBuffer.wrap(mapper.writeValueAsBytes(HeartBeat(currentPort, epochCounter))))
                    }
                    epochStarted = Instant.now()
                }
            }
        }

    }
}
