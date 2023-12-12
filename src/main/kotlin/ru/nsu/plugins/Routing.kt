package ru.nsu.plugins

import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import ru.nsu.algorithm.NodeState
import ru.nsu.algorithm.RaftMain
import java.util.concurrent.LinkedBlockingDeque

fun Application.configureRouting(blockingDeque: LinkedBlockingDeque<String>, raftMain: RaftMain) {
    routing {
        get("/storage/{key}") {
            call.parameters["key"]?.let { key ->
                raftMain.stateMachine.getFromState(key)?.let {
                    call.respondText(it)
                }
            }
            call.respondText("null")
        }

        post("/storage/{key}/{value}") {
            if (raftMain.raftState.state != NodeState.LEADER) {
                call.respondText("Leader is ${raftMain.raftState.leader}")
            }
            blockingDeque.add("set ${call.parameters["key"]} ${call.parameters["value"]}")
            call.respondText("OK")
        }

        post("/lock/acquire") {
            blockingDeque.add("acquire_lock")
            call.respondText("request created")
        }

        post("/lock/release") {
            blockingDeque.add("release_lock")
            call.respondText("request created")
        }

        delete("/log") {

        }

        get("/storage") {
            call.respond(raftMain.stateMachine.map)
        }

        get("/log") {
            call.respond(raftMain.logJournal.lsJournal())
        }
    }
}
