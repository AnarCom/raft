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
import java.util.concurrent.ConcurrentHashMap
import kotlin.Exception
import kotlin.concurrent.thread

const val ELECTION_TIMEOUT_MS = 1000

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
                writeToSocketChanel(client, JsonMessage(currentPort, 0U), mapper)
                val key = client.register(selector, SelectionKey.OP_READ)
                key.attach(it)
                connections[it] = client
            }
        }
        .map {
            try {
                it.join()
            } catch (_: Exception) {
            }
        }
}


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
                if (this !is HeartBeat) {
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
            println("write")
            formatHexDump(buffer.array())
        }
        socketChannel.write(
            buffer
        )
    } catch (e: Exception) {
        println("catch on sending: $e")
    }
}


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
    val connections = ConcurrentHashMap<Int, SocketChannel>()
    val mapper = jacksonObjectMapper()
    connectToAll(currentPort, nPrograms, selector, connections)
    Thread.sleep(100)
    println("debug sleep finished")
    var epochStarted = Instant.now()
    var epochCounter: ULong = 0U
    var nodeState = ProgramState.FOLLOWER
    var master = 0
    // if == -1 -> мы не запрашивали голосование -> игнорируем запросы
    var votesCounter = -1

    val bufferedReader = System.`in`.bufferedReader()

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
                if (messageData == null) {
                    if (key.attachment() != null) {
                        println("detached ${key.attachment()}")
                        val port = key.attachment() as Int
                        connections.remove(port)
                    }
                    client.close()
                } else {
                   // todo: add check for epoch
                    when (messageData) {
                        is HeartBeat -> {
                            epochStarted = Instant.now()
                            master = messageData.nodeIdentification
                        }

                        is VoteRequest -> {
                            val voteResult = messageData.epochCount.let { messageEpoch ->
                                if (messageEpoch > epochCounter) {
                                    epochCounter = messageEpoch
                                    true
                                } else {
                                    false
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
                            epochStarted = Instant.now()
                        }

                        is VoteAnswer -> {
                            if (votesCounter != -1 && messageData.voteResult) {
                                votesCounter++
                            }
                            val quota = (nPrograms.toDouble() / 2.toDouble()).toInt() + 1
                            println("quota $quota")
                            if (votesCounter >= quota) {
                                nodeState = ProgramState.LEADER
                                votesCounter = -1
                                epochStarted = Instant.now()
                                println("I am now leader")
                            }
                        }

                        is JsonMessage -> {
                            println("get new connection from ${messageData.nodeIdentification}")
                            connections[messageData.nodeIdentification] = client
                            key.attach(messageData.nodeIdentification)
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
                    }

                    "get" -> {
                    }

                    "cas" -> {
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
                    }
                    epochStarted = Instant.now()
                }
            }
        }
    }
}
