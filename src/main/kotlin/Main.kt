import algorithm.LogJournal
import algorithm.NodeState
import algorithm.RaftState
import dto.*
import kotlinx.serialization.json.Json
import utils.formatHexDump
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.util.concurrent.ConcurrentHashMap
import kotlin.concurrent.thread

fun startServer(self: NodeInformation, selector: Selector): ServerSocketChannel {
    val serverSocketChannel = ServerSocketChannel.open()
    serverSocketChannel.bind(InetSocketAddress(self.host, self.port))
    serverSocketChannel.configureBlocking(false)
    serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT)
    println("Server started")
    return serverSocketChannel
}

fun writeToSocketChanel(
    socketChannel: SocketChannel,
    data: BaseMessage,
) {
    try {
        val bytes = serialize(data).toByteArray()
        val buffer = ByteBuffer.allocate(Int.SIZE_BYTES + bytes.size).apply {
            putInt(bytes.size)
            put(bytes)
            flip()
        }
        if (data !is HeartBeatRequest && data !is HeartBeatResponse) {
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

fun writeToAll(data: BaseMessage) {
    connections.values.forEach {
        val chanel = it.channel() as SocketChannel
        writeToSocketChanel(
            chanel,
            data
        )
    }
}

fun readFromSocket(connection: SocketChannel): BaseMessage? =
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
            Json.decodeFromString<BaseMessage>(str).apply {
                if (this !is HeartBeatRequest) {
                    println(str)
                }
            }
        }
    } catch (e: Exception) {
        println("exception on reading: $e")
        null
    }

fun connectToAll(
    self: NodeInformation,
    other: List<NodeInformation>,
    selector: Selector
) {
    other.map {
        thread {
            val client = SocketChannel.open(InetSocketAddress(it.host, it.port))
            client.configureBlocking(false)
            writeToSocketChanel(client, HandShake(self, 0U))
            val key = client.register(selector, SelectionKey.OP_READ)
            key.attach(ConnectionDto(it, key))
            connections[it] = key
        }
    }.map {
        try {
            it.join()
        } catch (_: Exception) {

        }
    }
}

val connections = ConcurrentHashMap<NodeInformation, SelectionKey>()

fun main(args: Array<String>) {
    val nods = (0..<args.size / 2)
        .map {
            val port = args[it * 2 + 1].toInt()
            val host = args[it * 2]
            NodeInformation(port, host)
        }
        .toList()

    val raftState = RaftState(nods.first())
    val logJournal = LogJournal(raftState)

    val selector = Selector.open()
    val serverSocket = startServer(nods.first(), selector)
    connectToAll(nods.first(), nods.slice(1..<nods.size).toList(), selector)
    while (true) {
        selector.select(100)
        selector.selectedKeys().iterator().let {
            while (it.hasNext()) {
                val key = it.next()
                if (key.isAcceptable) {
                    val client = serverSocket.accept()
                    client.configureBlocking(false)
                    client.register(selector, SelectionKey.OP_READ)
                    println("connected")
                } else if (key.isReadable) {
                    val client = key.channel() as SocketChannel
                    val messageData = readFromSocket(client)
                    if (messageData == null) {
                        key.attachment()?.let { attachment ->
                            if (attachment is ConnectionDto) {
                                println("disconnected ${attachment.connection.host} ${attachment.connection.port}")
                                connections.remove(attachment.connection)
                            }
                            client.close()
                        }
                    } else {
                        when (messageData) {
                            is HandShake -> {
                                println("got new connection from ${messageData.nodeInformation.host} ${messageData.nodeInformation.port}")
                                connections[messageData.nodeInformation] = key
                                key.attach(
                                    ConnectionDto(messageData.nodeInformation, key, logJournal.getLastLogIndex())
                                )
                            }

                            is VoteRequest -> {
                                if (raftState.term < messageData.term && raftState.electedFor == null) {
                                    raftState.incTerm()
                                    raftState.electedFor = (key.attachment() as ConnectionDto).connection
                                    writeToSocketChanel(
                                        client,
                                        VoteResponse(raftState.self, raftState.term, true)
                                    )
                                } else {
                                    writeToSocketChanel(
                                        client,
                                        VoteResponse(raftState.self, raftState.term, false)
                                    )
                                }
                            }

                            is VoteResponse -> {
                                if (messageData.answer) {
                                    raftState.votes++
                                    //TODO: add +1
                                    if (raftState.votes >= ((nods.size / 2))) {
                                        raftState.state = NodeState.LEADER
                                        raftState.votes = 0
                                        println("I am now leader")
                                    }
                                }
                            }

                            is HeartBeatRequest -> {
                                if (messageData.term > raftState.term) {
                                    raftState.state = NodeState.FOLLOWER
                                }
                                raftState.resetTime()
                            }

                            else -> {
                                println("something strange")
                            }
                        }
                    }
                }
                it.remove()
            }

            when (raftState.state) {
                NodeState.LEADER -> {
                    if (raftState.isTimeToSendHeartBeat()) {
                        raftState.resetTime()
                        val heartBeatRequest = HeartBeatRequest(
                            raftState.self,
                            raftState.term,
                            0,
                            0U,
                            listOf(),
                            0
                        )
                        writeToAll(heartBeatRequest)

                    }

                }

                NodeState.CANDIDATE -> {
                    if (raftState.isElectionTimeout()) {
                        raftState.state = NodeState.FOLLOWER
                    }
                }

                NodeState.FOLLOWER -> {
                    if (raftState.isLeaderDead()) {
                        raftState.state = NodeState.CANDIDATE
                        raftState.incTerm()
                        raftState.votes = 1
                        val voteRequest = VoteRequest(
                            raftState.self,
                            raftState.term,
                            0,
                            0U
                        )
                        writeToAll(voteRequest)
                    }
                }
            }
        }
    }
}
