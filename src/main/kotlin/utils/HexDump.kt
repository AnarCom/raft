package utils

import java.io.UnsupportedEncodingException
import kotlin.math.min

fun formatHexDump(array: ByteArray) {
    val width = 16
    val builder = StringBuilder()
    var rowOffset = 0
    while (rowOffset < array.size) {
        builder.append(String.format("%06d:  ", rowOffset))
        for (index in 0 until width) {
            if (rowOffset + index < array.size) {
                builder.append(String.format("%02x ", array[rowOffset + index]))
            } else {
                builder.append("   ")
            }
        }
        if (rowOffset < array.size) {
            val asciiWidth = min(width.toDouble(), (array.size - rowOffset).toDouble()).toInt()
            builder.append("  |  ")
            try {
                builder.append(
                    String(array, rowOffset, asciiWidth, charset("UTF-8")).replace("\r\n".toRegex(), " ")
                        .replace("\n".toRegex(), " ")
                )
            } catch (ignored: UnsupportedEncodingException) {
                builder.append("^")
                //If UTF-8 isn't available as an encoding then what can we do?!
            }
        }
        builder.append(String.format("%n"))
        rowOffset += width
    }
    println(builder.toString())
}
