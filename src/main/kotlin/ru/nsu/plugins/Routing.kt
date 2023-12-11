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
            if(raftMain.raftState.state != NodeState.LEADER) {
                call.respondText("Leader is ${raftMain.raftState.leader}")
            }
            blockingDeque.add("set ${call.parameters["key"]} ${call.parameters["value"]}")
            call.respondText("OK")
        }

        delete("/log") {

        }

        get("/log") {
            call.respond(raftMain.logJournal.lsJournal())
        }
    }
}
