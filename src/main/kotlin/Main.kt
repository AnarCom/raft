import algorithm.NodeState
import algorithm.RaftState
import dto.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.internal.readJson
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
            key.attach(ConnectionDto(it))
            connections[it] = client
        }
    }.map {
        try {
            it.join()
        } catch (_: Exception) {

        }
    }
}

val connections = ConcurrentHashMap<NodeInformation, SocketChannel>()

fun main(args: Array<String>) {
    val nods = (0..<args.size / 2)
        .map {
            val port = args[it * 2 + 1].toInt()
            val host = args[it * 2]
            NodeInformation(port, host)
        }
        .toList()

    val raftState = RaftState()
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
                                connections[messageData.nodeInformation] = client
                                key.attach(ConnectionDto(messageData.nodeInformation))
                            }
                            else -> {
                                println("unsu")
                            }
                        }
                    }
                }
                it.remove()
            }

            when(raftState.state) {
                NodeState.LEADER -> {

                }
                NodeState.CANDIDATE -> {

                }
                NodeState.FOLLOWER -> {

                }
            }

        }
    }
    //connect to other
}
