package ru.nsu

import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import ru.nsu.algorithm.RaftMain
import ru.nsu.dto.NodeInformation
import ru.nsu.plugins.*
import java.util.concurrent.LinkedBlockingDeque

val blockingDeque = LinkedBlockingDeque<String>()
lateinit var raftMain: RaftMain
fun main() {
    val serverPort = System.getenv("SERVER_PORT").toInt()
    val anotherServers = System.getenv("ANOTHER_SERVERS").split(",").map {
        it.split(":").let { connectionInfo ->
            NodeInformation(connectionInfo.last().toInt(), connectionInfo.first())
        }
    }



    raftMain = RaftMain(
        anotherServers.first(),
        anotherServers.slice((1..<anotherServers.size)),
        serverPort,
        blockingDeque
    )
    raftMain.start()
    embeddedServer(Netty, port = serverPort, host = "0.0.0.0", module = Application::module)
        .start(wait = true)
}

fun Application.module() {
    configureSerialization()
    configureRouting(blockingDeque, raftMain)
}
