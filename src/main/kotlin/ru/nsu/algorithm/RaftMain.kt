package ru.nsu.algorithm

import kotlinx.serialization.json.Json
import ru.nsu.dto.*
import ru.nsu.utils.formatHexDump
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingDeque
import kotlin.concurrent.thread


class RaftMain(
    private val self: NodeInformation,
    private val others: List<NodeInformation>,
    private val serverPort: Int,
    private val blockingDeque: LinkedBlockingDeque<String>
) : Thread() {
    val raftState = RaftState(self)
    val stateMachine = StateMachine()
    val logJournal = LogJournal(raftState, stateMachine)

    override fun run() {
        sleep((serverPort.toLong() % 10) * 1000)
        val selector = Selector.open()
        val serverSocket = startServer(self, selector)
        connectToAll(self, others, selector)
        val quota = ((others.size + 1) / 2) + 1
        raftState.resetTime()
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
                                    if (raftState.term < messageData.term && raftState.electedFor == null && messageData.lastLogIndex >= logJournal.getLastLogIndex()) {
                                        raftState.incTerm()
                                        raftState.state = NodeState.FOLLOWER
                                        raftState.electedFor = (key.attachment() as ConnectionDto).connection
                                        writeToSocketChanel(
                                            client, VoteResponse(raftState.self, raftState.term, true)
                                        )
                                    } else {
                                        writeToSocketChanel(
                                            client, VoteResponse(raftState.self, raftState.term, false)
                                        )
                                    }
                                }

                                is VoteResponse -> {
                                    if (messageData.answer) {
                                        raftState.votes++
                                        if (raftState.votes >= quota) {
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
                                    raftState.leader = messageData.nodeInformation
                                    val ans = logJournal.appendEntities(messageData)
                                    raftState.resetTime()
                                    writeToSelectionKey(
                                        key, HeartBeatResponse(
                                            raftState.self, raftState.term, ans
                                        )
                                    )
                                }

                                is HeartBeatResponse -> {
                                    val attachment = connections.keys.filter { node ->
                                        node.host == messageData.nodeInformation.host && node.port == messageData.nodeInformation.port
                                    }.map { node ->
                                        connections[node]
                                    }.first()!!.attachment()

                                    val data = attachment.castToConnectionDto()

                                    if (data.lastSentFor == null) {
                                        TODO("last send for cannot be null")
                                    }

                                    if (!messageData.success) {
                                        if (data.nodeIndex > 0) {
                                            data.nodeIndex--
                                        }
                                        data.lastSentFor = null
                                    } else {
                                        data.nodeIndex = data.lastSentFor!!
                                        logJournal.processResponse(
                                            data.connection.host, data.connection.port, data.nodeIndex, quota
                                        )
                                    }
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
                            connections.values.forEach { key ->
                                val (request, nextIndex) = logJournal.createRequest(
                                    key.attachment().castToConnectionDto().nodeIndex
                                )
                                key.attachment().castToConnectionDto().lastSentFor = nextIndex
                                writeToSelectionKey(
                                    key,
                                    request,
                                )
                            }
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
                                logJournal.getLastLogIndex(),
                                logJournal.getLastLogTerm()
                            )
                            writeToAll(voteRequest)
                        }
                    }
                }
            }
            if (blockingDeque.isNotEmpty()) {
                val line = blockingDeque.take()
                val (command, arguments) = line.split(" ").let {
                    Pair(
                        it.first(), if (it.size > 1) {
                            it.slice(1..<it.size).toList()
                        } else {
                            emptyList()
                        }
                    )
                }
                when (command) {
                    "set" -> {
                        logJournal.addEntity(
                            command, arguments
                        )
                    }

                    "clear" -> {
                        logJournal.clearLog()
                        stateMachine.clearState()
                    }

                    else -> {
                        println("Unsupported operation")
                    }
                }
            }
        }
    }

    private fun startServer(self: NodeInformation, selector: Selector): ServerSocketChannel {
        val serverSocketChannel = ServerSocketChannel.open()
        serverSocketChannel.bind(InetSocketAddress(self.host, self.port))
        serverSocketChannel.configureBlocking(false)
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT)
        println("Server started")
        return serverSocketChannel
    }


    private fun connectToAll(
        self: NodeInformation, other: List<NodeInformation>, selector: Selector
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

    private fun writeToSocketChanel(
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

    private fun writeToSelectionKey(
        selectionKey: SelectionKey, data: BaseMessage
    ) {
        val chanel = selectionKey.channel() as SocketChannel
        writeToSocketChanel(chanel, data)

    }

    private fun readFromSocket(connection: SocketChannel): BaseMessage? = try {
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
                if (this !is HeartBeatRequest && this !is HeartBeatResponse) {
                    println(str)
                }
            }
        }
    } catch (e: Exception) {
        println("exception on reading: $e")
        null
    }

    private val connections = ConcurrentHashMap<NodeInformation, SelectionKey>()
    private fun writeToAll(data: BaseMessage) {
        connections.values.forEach {
            val chanel = it.channel() as SocketChannel
            writeToSocketChanel(
                chanel,
                data
            )
        }
    }
}
