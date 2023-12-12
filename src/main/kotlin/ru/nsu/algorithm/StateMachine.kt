package ru.nsu.algorithm

class StateMachine {
    val map: MutableMap<String, String> = mutableMapOf()

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

            "set_if_null" -> {
                if (arguments.size != 2) {
                    println("cannot apply set_if_null")
                } else {
                    if (map[arguments[0]] == null) {
                        map[arguments[0]] = arguments[1]
                        println("set_if_null successes")
                    }
                }
            }

            "delete_key" -> {
                if (arguments.size != 1) {
                    println("cannot apply delete_key")
                } else {
                    map.remove(arguments[0])
                }
            }

            "delete_cas" -> {
                if (arguments.size != 2) {
                    println("cannot apply delete_cas")
                } else {
                    if(map[arguments[0]] == arguments[1]) {
                        map.remove(arguments[0])
                        println("deleted key")
                    }
                }
            }

            "cas" -> {
                if (arguments.size != 3) {
                    println("cannot apply cas")
                } else {
                    if (map[arguments[0]] == arguments[2]) {
                        map[arguments[0]] = arguments[1]
                        println("cas success")
                    }
                }
            }

            else -> println("unsupported operation for state machine")
        }
    }

    fun getFromState(key: String): String? = map[key]

}
