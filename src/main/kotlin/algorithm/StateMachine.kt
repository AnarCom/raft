package algorithm

class StateMachine {
    private val map: MutableMap<String, String> = mutableMapOf()

    fun clearState() {
        map.clear()
    }

    fun applyState(command: String, arguments: List<String>) {
        when (command) {
            "set" -> {
                if (arguments.size != 2) {
                    println("cannot apply set")
                } else {
                    map[arguments[0]] = arguments[1]
                }
            }
            "cas" -> {
                if(arguments.size != 3) {
                    println("cannot apply cas")
                } else {
                    if(map[arguments[0]] == arguments[1]) {
                        map[arguments[0]] = arguments[2]
                    }
                }

            }
            else -> println("unsupported operation for state machine")
        }
    }

    fun getFromState(key: String): String? = map[key]

}
