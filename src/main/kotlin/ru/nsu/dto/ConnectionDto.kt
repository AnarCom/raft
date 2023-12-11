package ru.nsu.dto

import java.nio.channels.SelectionKey

class ConnectionDto(
    val connection: NodeInformation,
    var selectionKey: SelectionKey,
    var nodeIndex: Int = 0,
    var lastSentFor: Int? = null
)

fun Any.castToConnectionDto() = this as ConnectionDto
