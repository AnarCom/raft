class LogStorage(
    val nodeId: Int
) {
    private val logs: MutableList<LogEntity> = mutableListOf()
    private val cache: MutableMap<String, String> = mutableMapOf()

    //    fun addLog(logEntity: LogEntity) {
//        logs.add(logEntity)
//        cache[logEntity.key] = logEntity.value
//    }
    fun addLog(key: String, value: String, epochCount: ULong): LogEntity {
        val logEntity = LogEntity(
            nodeId,
            epochCount,
            key,
            value,
            logs.size
        )
        addLog(logEntity)
        return logEntity
    }

    fun addLog(logEntity: LogEntity): Boolean =
        if (logs.size == logEntity.index) {
            logs.add(logEntity)
            cache[logEntity.key] = logEntity.value
            true
        } else {
            false
        }

    fun casOperation(casRequest: CasRequest, epochCount: ULong): LogEntity? =
        if (cache[casRequest.key] == casRequest.expectedValue) {
            addLog(casRequest.key, casRequest.value, epochCount)
        } else {
            null
        }

    fun revalidateLog(list: LogJournal) {
        logs.clear()
        cache.clear()
        for (i in list.logs) {
            addLog(i)
        }
    }

    fun getValue(key: String): String? = cache[key]

    fun getAllLogs(): List<LogEntity> = logs
}
