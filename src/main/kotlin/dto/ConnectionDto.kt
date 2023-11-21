package dto

import java.nio.channels.SelectionKey

class ConnectionDto(
    val connection: NodeInformation,
    var selectionKey: SelectionKey,
    val nodeIndex: Int = 0,
)

fun Any.castToConnectionDto() = this as ConnectionDto
