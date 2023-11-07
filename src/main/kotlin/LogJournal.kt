class LogJournal(
    private val raftState: RaftState,
) {
    private val journal: MutableList<Pair<Boolean, LogEntry>> = mutableListOf()

    fun addLogEntry(

    )

//    fun addLogEntry(
//        entryIndex: Int,
//        command: String,
//        arguments: Array<String>,
//        addedBy: Int
//    ): Boolean =
//        if (entryIndex > journal.size) {
//            false
//        } else if (entryIndex == journal.size) {
//            journal.add(
//                LogEntry(
//                    command, arguments, addedBy
//                )
//            )
//            true
//        } else {
//            journal.slice(0..<entryIndex).let {
//                journal.clear()
//                journal.addAll(it)
//            }
//            true
//        }
}

private class LogEntry(
    val command: String,
    val arguments: Array<String>,
    val addedBy: Int,
    val term: ULong,
    var sended: Boolean,
)
